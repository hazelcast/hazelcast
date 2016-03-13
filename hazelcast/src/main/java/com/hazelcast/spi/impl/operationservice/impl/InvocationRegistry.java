/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.partition.ReplicaErrorLogger;
import com.hazelcast.internal.util.counters.MwCounter;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.util.counters.MwCounter.newMwCounter;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static com.hazelcast.spi.Operation.CALL_ID_LOCAL_SKIPPED;
import static com.hazelcast.spi.OperationAccessor.setCallId;

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
public class InvocationRegistry implements Iterable<Invocation> {

    private static final int INITIAL_CAPACITY = 1000;
    private static final float LOAD_FACTOR = 0.75f;
    private static final double HUNDRED_PERCENT = 100d;

    @Probe(name = "invocations.pending", level = MANDATORY)
    private final ConcurrentMap<Long, Invocation> invocations;
    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;
    private final CallIdSequence callIdSequence;

    @Probe(name = "responses[normal]", level = MANDATORY)
    private final SwCounter responsesNormal = newSwCounter();
    @Probe(name = "responses[timeout]", level = MANDATORY)
    private final SwCounter responsesTimeout = newSwCounter();
    @Probe(name = "responses[backup]", level = MANDATORY)
    private final MwCounter responsesBackup = newMwCounter();
    @Probe(name = "responses[error]", level = MANDATORY)
    private final SwCounter responsesError = newSwCounter();
    @Probe(name = "responses[missing]", level = MANDATORY)
    private final MwCounter responsesMissing = newMwCounter();

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

    @Override
    public Iterator<Invocation> iterator() {
        return invocations.values().iterator();
    }

    /**
     * Intention to expose the entry set is to mutate it.
     *
     * @return set of invocations in this registry
     */
    public Set<Map.Entry<Long, Invocation>> entrySet() {
        return invocations.entrySet();
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


    public void notifyBackupComplete(long callId) {
        responsesBackup.inc();

        try {
            Invocation invocation = invocations.get(callId);

            // It can happen that a backup response is send without the Invocation being available anymore.
            // This is because the InvocationRegistry will automatically release invocations where the backup is
            // taking too much time.
            if (invocation == null) {
                if (logger.isFinestEnabled()) {
                    logger.finest("No Invocation found for backup response with callId " + callId);
                }
                return;
            }

            invocation.notifySingleBackupComplete();
        } catch (Exception e) {
            ReplicaErrorLogger.log(e, logger);
        }
    }

    public void notifyErrorResponse(long callId, Object cause, Address sender) {
        responsesError.inc();
        Invocation invocation = invocations.get(callId);

        if (invocation == null) {
            responsesMissing.inc();
            if (nodeEngine.isRunning()) {
                logger.warning("No Invocation found for error response with callId: " + callId + " sent from " + sender);
            }
            return;
        }

        invocation.notifyError(cause);
    }

    public void notifyNormalResponse(long callId, Object value, int backupCount, Address sender) {
        responsesNormal.inc();
        Invocation invocation = invocations.get(callId);

        if (invocation == null) {
            responsesMissing.inc();
            if (nodeEngine.isRunning()) {
                logger.warning("No Invocation found for normal response with callId " + callId + " sent from " + sender);
            }
            return;
        }
        invocation.notifyNormalResponse(value, backupCount);
    }

    public void notifyCallTimeout(long callId, Address sender) {
        responsesTimeout.inc();
        Invocation invocation = invocations.get(callId);

        if (invocation == null) {
            responsesMissing.inc();
            if (nodeEngine.isRunning()) {
                logger.warning("No Invocation found for call timeout response with callId" + callId + " sent from " + sender);
            }
            return;
        }
        invocation.notifyCallTimeout();
    }

    public void reset() {
        for (Invocation invocation : this) {
            try {
                invocation.notifyError(new MemberLeftException());
            } catch (Throwable e) {
                logger.warning(invocation + " could not be notified with reset message -> " + e.getMessage());
            }
        }
    }

    public void shutdown() {
        for (Invocation invocation : this) {
            try {
                invocation.notifyError(new HazelcastInstanceNotActiveException());
            } catch (Throwable e) {
                logger.warning(invocation + " could not be notified with shutdown message -> " + e.getMessage(), e);
            }
        }
    }

}
