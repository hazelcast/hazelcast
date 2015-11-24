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
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.ReplicaErrorLogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.responses.BackupResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.CallTimeoutResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.ErrorResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.Response;
import com.hazelcast.util.counters.MwCounter;
import com.hazelcast.util.counters.SwCounter;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.spi.Operation.CALL_ID_LOCAL_SKIPPED;
import static com.hazelcast.spi.OperationAccessor.setCallId;
import static com.hazelcast.util.counters.MwCounter.newMwCounter;
import static com.hazelcast.util.counters.SwCounter.newSwCounter;

/**
 * The InvocationsRegistry is responsible for the registration of all pending invocations. Using the InvocationRegistry the
 * Invocation and its response(s) can be linked to each other.
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

    private static final int INITIAL_CAPACITY = 1000;
    private static final float LOAD_FACTOR = 0.75f;
    private static final double HUNDRED_PERCENT = 100d;

    @Probe(name = "invocations.pending", level = MANDATORY)
    private final ConcurrentMap<Long, Invocation> invocations;
    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;
    private final CallIdSequence callIdSequence;

    @Probe(name = "response.normal.count", level = MANDATORY)
    private final SwCounter responseNormalCounter = newSwCounter();
    @Probe(name = "response.timeout.count", level = MANDATORY)
    private final SwCounter responseTimeoutCounter = newSwCounter();
    @Probe(name = "response.backup.count", level = MANDATORY)
    private final MwCounter responseBackupCounter = newMwCounter();
    @Probe(name = "response.error.count", level = MANDATORY)
    private final SwCounter responseErrorCounter = newSwCounter();

    public InvocationRegistry(NodeEngineImpl nodeEngine, ILogger logger, BackpressureRegulator backpressureRegulator,
                              int concurrencyLevel) {
        this.nodeEngine = nodeEngine;
        this.logger = logger;
        this.callIdSequence = backpressureRegulator.newCallIdSequence();
        this.invocations = new ConcurrentHashMap<Long, Invocation>(INITIAL_CAPACITY, LOAD_FACTOR, concurrencyLevel);

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
        assert invocation.op.getCallId() == 0 : "can't register twice: " + invocation;

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
        if (!deleted && logger.isFinestEnabled()) {
            logger.finest("failed to deregister callId: " + callId + " " + invocation);
        }
    }

    /**
     * Returns the number of pending invocations.
     *
     * @return the number of pending invocations.
     */
    public int size() {
        return invocations.size();
    }

    public Collection<Invocation> invocations() {
        return invocations.values();
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
     * @param sender   Endpoint who sent the response
     */
    public void notify(Response response, Address sender) {
        if (response instanceof NormalResponse) {
            notifyNormalResponse((NormalResponse) response, sender);
        } else if (response instanceof BackupResponse) {
            notifyBackupComplete(response.getCallId());
        } else if (response instanceof CallTimeoutResponse) {
            notifyCallTimeout((CallTimeoutResponse) response, sender);
        } else if (response instanceof ErrorResponse) {
            notifyErrorResponse((ErrorResponse) response, sender);
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

    private void notifyErrorResponse(ErrorResponse response, Address sender) {
        responseErrorCounter.inc();
        Invocation invocation = invocations.get(response.getCallId());

        if (invocation == null) {
            if (nodeEngine.isRunning()) {
                logger.warning("No Invocation found for response: " + response + " sent from " + sender);
            }
            return;
        }

        invocation.notifyError(response.getCause());
    }

    private void notifyNormalResponse(NormalResponse response, Address sender) {
        responseNormalCounter.inc();
        Invocation invocation = invocations.get(response.getCallId());

        if (invocation == null) {
            if (nodeEngine.isRunning()) {
                logger.warning("No Invocation found for response: " + response + " sent from " + sender);
            }
            return;
        }
        invocation.notifyNormalResponse(response.getValue(), response.getBackupCount());
    }

    private void notifyCallTimeout(CallTimeoutResponse response, Address sender) {
        responseTimeoutCounter.inc();
        Invocation invocation = invocations.get(response.getCallId());

        if (invocation == null) {
            if (nodeEngine.isRunning()) {
                logger.warning("No Invocation found for response: " + response + " sent from " + sender);
            }
            return;
        }
        invocation.notifyCallTimeout();
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
        for (Invocation invocation : invocations.values()) {
            try {
                invocation.notifyError(new HazelcastInstanceNotActiveException());
            } catch (Throwable e) {
                logger.warning(invocation + " could not be notified with shutdown message -> " + e.getMessage(), e);
            }
        }
    }

}
