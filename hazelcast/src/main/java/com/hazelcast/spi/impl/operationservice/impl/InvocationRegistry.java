/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.ReplicaErrorLogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.InternalExecutionService;
import com.hazelcast.spi.impl.operationexecutor.OperationHostileThread;
import com.hazelcast.spi.impl.operationservice.impl.responses.BackupResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.CallTimeoutResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.ErrorResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.Response;
import com.hazelcast.util.Clock;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.counters.MwCounter;
import com.hazelcast.util.counters.SwCounter;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.inspectOutputMemoryError;
import static com.hazelcast.spi.Operation.CALL_ID_LOCAL_SKIPPED;
import static com.hazelcast.spi.OperationAccessor.setCallId;
import static com.hazelcast.util.counters.MwCounter.newMwCounter;
import static com.hazelcast.util.counters.SwCounter.newSwCounter;

/**
 * The InvocationsRegistry is responsible for the registration of all pending invocations.
 * <p/>
 * When an invocation is registered, a callId is determined. Based on this call-id, when a
 * {@link com.hazelcast.spi.impl.operationservice.impl.responses.Response} comes in, the
 * appropriate invocation can be looked up.
 * <p/>
 * Some idea's:
 * - use an ringbuffer to store all invocations instead of a CHM. The call-id can be used as sequence-id for this
 * ringbuffer. It can be that you run in slots that have not been released; if that happens, just keep increasing
 * the sequence (although you now get sequence-gaps).
 * - pre-allocate all invocations. Because the ringbuffer has a fixed capacity, pre-allocation should be easy. Also
 * the PartitionInvocation and TargetInvocation can be folded into Invocation.
 */
public class InvocationRegistry {

    private static final long SCHEDULE_DELAY = 1111;
    private static final int INITIAL_CAPACITY = 1000;
    private static final float LOAD_FACTOR = 0.75f;
    private static final int DELAY_MILLIS = 1000;
    private static final double HUNDRED_PERCENT = 100d;

    private final long backupTimeoutMillis;

    @Probe(name = "invocations.pending")
    private final ConcurrentMap<Long, Invocation> invocations;
    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;
    private final InspectionThread inspectionThread;
    private final CallIdSequence callIdSequence;
    private final long slowInvocationThresholdMs;

    @Probe(name = "response.normal.count")
    private final SwCounter responseNormalCounter = newSwCounter();
    @Probe(name = "response.timeout.count")
    private final SwCounter responseTimeoutCounter = newSwCounter();
    @Probe(name = "response.backup.count")
    private final MwCounter responseBackupCounter = newMwCounter();
    @Probe(name = "response.error.count")
    private final SwCounter responseErrorCounter = newSwCounter();
    @Probe(name = "invocations.backupTimeouts")
    private final SwCounter backupTimeoutsCount = newSwCounter();
    @Probe(name = "invocations.normalTimeouts")
    private final SwCounter normalTimeoutsCount = newSwCounter();

    public InvocationRegistry(NodeEngineImpl nodeEngine, ILogger logger, BackpressureRegulator backpressureRegulator,
                              int concurrencyLevel) {
        this.nodeEngine = nodeEngine;
        this.logger = logger;
        this.callIdSequence = backpressureRegulator.newCallIdSequence();
        GroupProperties props = nodeEngine.getGroupProperties();
        this.slowInvocationThresholdMs = initSlowInvocationThresholdMs(props);
        this.backupTimeoutMillis = props.OPERATION_BACKUP_TIMEOUT_MILLIS.getLong();
        this.invocations = new ConcurrentHashMap<Long, Invocation>(INITIAL_CAPACITY, LOAD_FACTOR, concurrencyLevel);
        this.inspectionThread = new InspectionThread();

        inspectionThread.start();
        nodeEngine.getMetricsRegistry().scanAndRegister(this, "operation");
    }

    @Probe(name = "invocations.usedPercentage")
    private double invocationsUsedPercentage() {
        int maxConcurrentInvocations = callIdSequence.getMaxConcurrentInvocations();
        if (maxConcurrentInvocations == Integer.MAX_VALUE) {
            return 0;
        }

        return (HUNDRED_PERCENT * invocations.size()) / maxConcurrentInvocations;
    }

    private long initSlowInvocationThresholdMs(GroupProperties props) {
        long thresholdMs = props.SLOW_INVOCATION_DETECTOR_THRESHOLD_MILLIS.getLong();
        if (thresholdMs > -1) {
            logger.info("Slow invocation detector enabled, using threshold: " + thresholdMs + " ms");
        }
        return thresholdMs;
    }

    @Probe(name = "invocations.lastCallId")
    public long getLastCallId() {
        return callIdSequence.getLastCallId();
    }

    /**
     * Registers an invocation.
     *
     * @param invocation The invocation to register.
     */
    public void register(Invocation invocation) {
        assert invocation.op.getCallId() == 0 : "can't register twice:" + invocation;

        long callId = callIdSequence.next(invocation);
        setCallId(invocation.op, callId);

        if (callId == CALL_ID_LOCAL_SKIPPED) {
            return;
        }

        invocations.put(callId, invocation);
    }

    /**
     * Deregisters an invocation.
     * <p/>
     * If the invocation registration was skipped, the call is ignored.
     *
     * @param invocation The Invocation to deregister.
     */
    public void deregister(Invocation invocation) {
        long callId = invocation.op.getCallId();

        callIdSequence.complete(invocation);

        setCallId(invocation.op, 0);

        if (callId == 0 || callId == CALL_ID_LOCAL_SKIPPED) {
            return;
        }

        boolean deleted = invocations.remove(callId) != null;
        assert deleted : "failed to deregister callId:" + callId + " " + invocation;
    }

    /**
     * Returns the number of pending invocations.
     *
     * @return the number of pending invocations.
     */
    public int size() {
        return invocations.size();
    }

    /**
     * Gets the invocation for the given call id.
     *
     * @param callId the callId.
     * @return the Invocation for the given callId, or null if no invocation was found.
     */
    public Invocation get(long callId) {
        return invocations.get(callId);
    }

    /**
     * Notifies the invocation that a Response is available.
     *
     * @param response The response that is available.
     */
    public void notify(Response response) {
        if (response instanceof NormalResponse) {
            notifyNormalResponse((NormalResponse) response);
        } else if (response instanceof BackupResponse) {
            notifyBackupComplete(response.getCallId());
        } else if (response instanceof CallTimeoutResponse) {
            notifyCallTimeout((CallTimeoutResponse) response);
        } else if (response instanceof ErrorResponse) {
            notifyErrorResponse((ErrorResponse) response);
        } else {
            logger.severe("Unrecognized response: " + response);
        }
    }

    public void notifyBackupComplete(long callId) {
        responseBackupCounter.inc();

        try {
            Invocation invocation = invocations.get(callId);

            // It can happen that a {@link BackupResponse} is send without the Invocation being available anymore.
            // This is because the InvocationRegistry will automatically release invocations where the backup is
            // taking too much time.
            if (invocation == null) {
                if (logger.isFinestEnabled()) {
                    logger.finest("No Invocation found for BackupResponse with callId " + callId);
                }
                return;
            }

            invocation.notifySingleBackupComplete();
        } catch (Exception e) {
            ReplicaErrorLogger.log(e, logger);
        }
    }

    private void notifyErrorResponse(ErrorResponse response) {
        responseErrorCounter.inc();

        Invocation invocation = invocations.get(response.getCallId());

        if (invocation == null) {
            if (nodeEngine.isActive()) {
                logger.warning("No Invocation found for response: " + response);
            }
            return;
        }

        invocation.notifyError(response.getCause());
    }

    private void notifyNormalResponse(NormalResponse response) {
        responseNormalCounter.inc();

        Invocation invocation = invocations.get(response.getCallId());

        if (invocation == null) {
            if (nodeEngine.isActive()) {
                logger.warning("No Invocation found for response: " + response);
            }
            return;
        }
        invocation.notifyNormalResponse(response.getValue(), response.getBackupCount());
    }

    private void notifyCallTimeout(CallTimeoutResponse response) {
        responseTimeoutCounter.inc();

        Invocation invocation = invocations.get(response.getCallId());

        if (invocation == null) {
            if (nodeEngine.isActive()) {
                logger.warning("No Invocation found for response: " + response);
            }
            return;
        }
        invocation.notifyCallTimeout();
    }

    public void onMemberLeft(MemberImpl member) {
        // postpone notifying calls since real response may arrive in the mean time.
        InternalExecutionService executionService = nodeEngine.getExecutionService();
        Runnable task = new OnMemberLeftTask(member);
        executionService.schedule(task, SCHEDULE_DELAY, TimeUnit.MILLISECONDS);
    }

    public void reset() {
        for (Invocation invocation : invocations.values()) {
            try {
                invocation.notifyError(new MemberLeftException());
            } catch (Throwable e) {
                logger.warning(invocation + " could not be notified with reset message -> " + e.getMessage());
            }
        }
    }

    public void shutdown() {
        inspectionThread.shutdown();

        for (Invocation invocation : invocations.values()) {
            try {
                invocation.notifyError(new HazelcastInstanceNotActiveException());
            } catch (Throwable e) {
                logger.warning(invocation + " could not be notified with shutdown message -> " + e.getMessage(), e);
            }
        }
    }

    public void awaitTermination(long timeoutMillis) throws InterruptedException {
        inspectionThread.join(timeoutMillis);
    }

    /**
     * The InspectionThread iterates over all pending invocations and sees what needs to be done:
     * - currently it only checks for timeouts
     * <p/>
     * But it should also check if a 'is still running' check needs to be done. This removed complexity from
     * the invocation.waitForResponse which is too complicated too understand.
     *
     * This class needs to implement the OperationHostileThread interface to make sure that the OperationExecutor
     * is not going to schedule any operations on this task due to retry.
     */
    class InspectionThread extends Thread implements OperationHostileThread {

        private volatile boolean shutdown;

        InspectionThread() {
            super(nodeEngine.getNode().getHazelcastThreadGroup().getThreadNamePrefix("InspectInvocationsThread"));
        }

        public void shutdown() {
            shutdown = true;
            interrupt();
        }

        @Override
        public void run() {
            try {
                while (!shutdown) {
                    scanHandleOperationTimeout();
                    if (!shutdown) {
                        sleep();
                    }
                }
            } catch (Throwable t) {
                inspectOutputMemoryError(t);
                logger.severe("Failed to run", t);
            }
        }

        private void sleep() {
            try {
                Thread.sleep(DELAY_MILLIS);
            } catch (InterruptedException ignore) {
                // can safely be ignored. If this thread wants to shut down, it will read the shutdown variable.
                EmptyStatement.ignore(ignore);
            }
        }

        private void scanHandleOperationTimeout() {
            if (invocations.isEmpty()) {
                return;
            }

            long now = Clock.currentTimeMillis();
            int backupTimeouts = 0;
            int invocationTimeouts = 0;
            for (Invocation invocation : invocations.values()) {
                if (shutdown) {
                    return;
                }

                detectSlowInvocation(now, invocation);

                if (checkInvocationTimeout(invocation)) {
                    invocationTimeouts++;
                }

                if (checkBackupTimeout(invocation)) {
                    backupTimeouts++;
                }
            }

            backupTimeoutsCount.inc(backupTimeouts);
            normalTimeoutsCount.inc(invocationTimeouts);
            log(backupTimeouts, invocationTimeouts);
        }

        private void detectSlowInvocation(long now, Invocation invocation) {
            if (slowInvocationThresholdMs > 0) {
                long durationMs = now - invocation.op.getInvocationTime();
                if (durationMs > slowInvocationThresholdMs) {
                    logger.info("Slow invocation: duration=" + durationMs + " ms, operation="
                            + invocation.op.getClass().getName() + " inv:" + invocation);
                }
            }
        }

        private boolean checkInvocationTimeout(Invocation invocation) {
            try {
                return invocation.checkInvocationTimeout();
            } catch (Throwable t) {
                inspectOutputMemoryError(t);
                logger.severe("Failed to handle operation timeout of invocation:" + invocation, t);
                return false;
            }
        }

        private boolean checkBackupTimeout(Invocation invocation) {
            try {
                return invocation.checkBackupTimeout(backupTimeoutMillis);
            } catch (Throwable t) {
                inspectOutputMemoryError(t);
                logger.severe("Failed to handle backup timeout of invocation:" + invocation, t);
                return false;
            }
        }

        private void log(int backupTimeouts, int invocationTimeouts) {
            if (backupTimeouts > 0 || invocationTimeouts > 0) {
                logger.info("Handled " + invocationTimeouts + " invocation timeouts and " + backupTimeouts + " backupTimeouts");
            }
        }
    }

    private class OnMemberLeftTask implements Runnable {
        private final MemberImpl leftMember;

        public OnMemberLeftTask(MemberImpl leftMember) {
            this.leftMember = leftMember;
        }

        @Override
        public void run() {
            for (Invocation invocation : invocations.values()) {
                if (hasMemberLeft(invocation)) {
                    invocation.notifyError(new MemberLeftException(leftMember));
                }
            }
        }

        private boolean hasMemberLeft(Invocation invocation) {
            MemberImpl targetMember = invocation.targetMember;
            if (targetMember == null) {
                Address invTarget = invocation.invTarget;
                return leftMember.getAddress().equals(invTarget);
            } else {
                return leftMember.getUuid().equals(targetMember.getUuid());
            }
        }
    }
}
