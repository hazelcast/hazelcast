/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.HazelcastOverloadException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.diagnostics.InvocationProfilerPlugin;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.StaticMetricsProvider;
import com.hazelcast.internal.util.LatencyDistribution;
import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionIteratingOperation;
import com.hazelcast.spi.impl.sequence.CallIdSequence;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_METRIC_INVOCATION_REGISTRY_INVOCATIONS_LAST_CALL_ID;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_METRIC_INVOCATION_REGISTRY_INVOCATIONS_PENDING;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_METRIC_INVOCATION_REGISTRY_INVOCATIONS_USED_PERCENTAGE;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_PREFIX;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.metrics.ProbeUnit.PERCENT;
import static com.hazelcast.spi.impl.operationservice.OperationAccessor.deactivate;
import static com.hazelcast.spi.impl.operationservice.OperationAccessor.setCallId;

/**
 * Responsible for the registration of all pending invocations.
 * <p>
 * By using the InvocationRegistry, the Invocation and its response(s) can be linked to each other.
 * <p>
 * When an invocation is registered, a callId is determined. Based on this call ID, when a
 * {@link com.hazelcast.spi.impl.operationservice.impl.responses.Response} comes in, the
 * appropriate invocation can be looked up.
 * <p>
 * Some ideas:
 * <ul>
 * <li>Use a ringbuffer to store all invocations instead of a CHM. The call ID can be used as sequence ID for this
 * ringbuffer. It can be that you run in slots that have not been released; if that happens, just keep increasing
 * the sequence (although you now get sequence-gaps).</li>
 * <li>Pre-allocate all invocations. Because the ringbuffer has a fixed capacity, pre-allocation should be easy. Also
 * the PartitionInvocation and TargetInvocation can be folded into Invocation.</li>
 * </ul>
 */
public class InvocationRegistry implements Iterable<Invocation>, StaticMetricsProvider {

    private static final int CORE_SIZE_CHECK = 8;
    private static final int CORE_SIZE_FACTOR = 4;
    private static final int CONCURRENCY_LEVEL = 16;

    private static final int INITIAL_CAPACITY = 1000;
    private static final float LOAD_FACTOR = 0.75f;
    private static final double HUNDRED_PERCENT = 100d;

    @Probe(name = OPERATION_METRIC_INVOCATION_REGISTRY_INVOCATIONS_PENDING, level = MANDATORY)
    private final ConcurrentMap<Long, Invocation> invocations;
    private final ILogger logger;
    private final CallIdSequence callIdSequence;
    private final boolean profilerEnabled;
    private final ConcurrentMap<Class, LatencyDistribution> latencyDistributions = new ConcurrentHashMap<>();
    private volatile boolean alive = true;

    public InvocationRegistry(ILogger logger, CallIdSequence callIdSequence, HazelcastProperties properties) {
        this.logger = logger;
        this.callIdSequence = callIdSequence;

        int coreSize = RuntimeAvailableProcessors.get();
        boolean reallyMultiCore = coreSize >= CORE_SIZE_CHECK;
        int concurrencyLevel = reallyMultiCore ? coreSize * CORE_SIZE_FACTOR : CONCURRENCY_LEVEL;

        this.invocations = new ConcurrentHashMap<>(INITIAL_CAPACITY, LOAD_FACTOR, concurrencyLevel);
        this.profilerEnabled = properties.getInteger(InvocationProfilerPlugin.PERIOD_SECONDS) > 0;
    }

    @Override
    public void provideStaticMetrics(MetricsRegistry registry) {
        registry.registerStaticMetrics(this, OPERATION_PREFIX);
    }

    @Probe(name = OPERATION_METRIC_INVOCATION_REGISTRY_INVOCATIONS_USED_PERCENTAGE, unit = PERCENT)
    private double invocationsUsedPercentage() {
        int maxConcurrentInvocations = callIdSequence.getMaxConcurrentInvocations();
        if (maxConcurrentInvocations == Integer.MAX_VALUE) {
            return 0;
        }

        return (HUNDRED_PERCENT * invocations.size()) / maxConcurrentInvocations;
    }

    @Probe(name = OPERATION_METRIC_INVOCATION_REGISTRY_INVOCATIONS_LAST_CALL_ID)
    long getLastCallId() {
        return callIdSequence.getLastCallId();
    }

    /**
     * Registers an invocation.
     *
     * @param invocation The invocation to register.
     * @return {@code false} when InvocationRegistry is not alive and registration is not successful, {@code true} otherwise
     */
    public boolean register(Invocation invocation) {
        final long callId;
        boolean force = invocation.op.isUrgent() || invocation.isRetryCandidate();
        try {
            callId = force ? callIdSequence.forceNext() : callIdSequence.next();
        } catch (HazelcastOverloadException e) {
            throw new HazelcastOverloadException("Failed to start invocation due to overload: " + invocation, e);
        }
        try {
            // fails with IllegalStateException if the operation is already active
            setCallId(invocation.op, callId);
        } catch (IllegalStateException e) {
            callIdSequence.complete();
            throw e;
        }
        invocations.put(callId, invocation);
        if (!alive) {
            invocation.notifyError(new HazelcastInstanceNotActiveException());
            return false;
        }
        return true;
    }

    /**
     * Deregisters an invocation. If the associated operation is inactive, takes no action and returns {@code false}.
     * This ensures the idempotency of deregistration.
     *
     * @param invocation The Invocation to deregister.
     * @return {@code true} if this call deregistered the invocation; {@code false} if the invocation wasn't registered
     */
    public boolean deregister(Invocation invocation) {
        if (!deactivate(invocation.op)) {
            return false;
        }
        invocations.remove(invocation.op.getCallId());
        callIdSequence.complete();
        return true;
    }

    public void retire(Invocation invocation) {
        if (!profilerEnabled) {
            return;
        }

        Operation op = invocation.op;
        Class c = op.getClass();
        if (op instanceof PartitionIteratingOperation) {
            c = ((PartitionIteratingOperation) op).getOperationFactory().getClass();
        }
        LatencyDistribution distribution = latencyDistributions.computeIfAbsent(c, k -> new LatencyDistribution());
        distribution.done(invocation.firstInvocationTimeNanos);
    }

    public final ConcurrentMap<Class, LatencyDistribution> latencyDistributions() {
        return latencyDistributions;
    }


    /**
     * Returns the number of pending invocations.
     *
     * @return the number of pending invocations
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
     * Gets the invocation for the given call ID.
     *
     * @param callId the call ID
     * @return the Invocation for the given call ID, or {@code null} if no invocation was found.
     */
    public Invocation get(long callId) {
        return invocations.get(callId);
    }

    public void reset(Throwable cause) {
        for (Invocation invocation : this) {
            try {
                invocation.notifyError(new MemberLeftException(cause));
            } catch (Throwable e) {
                logger.warning(invocation + " could not be notified with reset message -> " + e.getMessage());
            }
        }
    }

    public void shutdown() {
        alive = false;

        for (Invocation invocation : this) {
            try {
                invocation.notifyError(new HazelcastInstanceNotActiveException());
            } catch (Throwable e) {
                logger.warning(invocation + " could not be notified with shutdown message -> " + e.getMessage(), e);
            }
        }
    }
}
