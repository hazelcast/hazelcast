package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.ReplicaErrorLogger;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.InternalExecutionService;
import com.hazelcast.spi.impl.operationservice.impl.responses.Response;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.spi.OperationAccessor.setCallId;

/**
 * The InvocationsRegistry is responsible for the registration of all pending invocations.
 * <p/>
 * When an invocation is registered, a callId is determined. Based on this call-id, when a
 * {@link com.hazelcast.spi.impl.operationservice.impl.responses.Response} comes in, the
 * appropriate invocation can be looked up.
 */
public class InvocationRegistry {
    private static final long SCHEDULE_DELAY = 1111;
    private static final int INITIAL_CAPACITY = 1000;
    private static final float LOAD_FACTOR = 0.75f;

    private final AtomicLong callIdGen = new AtomicLong(1);
    private final ConcurrentMap<Long, Invocation> invocations;
    private final OperationServiceImpl operationService;
    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;

    public InvocationRegistry(OperationServiceImpl operationService, int concurrencyLevel) {
        this.operationService = operationService;
        this.nodeEngine = operationService.nodeEngine;
        this.logger = operationService.logger;
        this.invocations = new ConcurrentHashMap<Long, Invocation>(INITIAL_CAPACITY, LOAD_FACTOR, concurrencyLevel);

    }

    public long nextCallId() {
        return callIdGen.get();
    }

    public void register(Invocation invocation) {
        long callId = callIdGen.getAndIncrement();
        Operation op = invocation.op;
        if (op.getCallId() != 0) {
            invocations.remove(op.getCallId());
        }

        invocations.put(callId, invocation);
        setCallId(invocation.op, callId);
    }

    public void deregister(Invocation invocation) {
        long callId = invocation.op.getCallId();
        // locally executed non backup-aware operations (e.g. a map.get on a local member) doesn't have a call id.
        // so in that case we can skip the deregistration since it isn't registered in the first place.
        if (callId == 0) {
            return;
        }

        invocations.remove(callId);
    }

    public Collection<Invocation> invocations() {
        return invocations.values();
    }

    /**
     * Returns the number of pending invocations.
     *
     * @return the number of the pending invocations.
     */
    public int size() {
        return invocations.size();
    }

    /**
     * Checks if there are any pending invocations.
     *
     * @return true if there are any pending invocations, false otherwise.
     */
    public boolean isEmpty() {
        return invocations.isEmpty();
    }

    /**
     * Gets the invocation for the given call id.
     *
     * @param callId the callId.
     * @return the found Invocation or null if no invocation was found.
     */
    public Invocation get(long callId) {
        return invocations.get(callId);
    }

    public void notifyBackupComplete(long callId) {
        try {
            Invocation invocation = invocations.get(callId);
            if (invocation == null) {
                return;
            }
            invocation.handleBackupComplete();
        } catch (Exception e) {
            ReplicaErrorLogger.log(e, logger);
        }
    }

    /**
     * Notifies the invocation that a Response is available.
     *
     * @param response
     */
    public void notify(Response response) {
        Invocation invocation = invocations.get(response.getCallId());

        if (invocation == null) {
            if (operationService.nodeEngine.isActive()) {
                logger.warning("No Invocation found for response: " + response);
            }
            return;
        }

        invocation.notify(response);
    }

    public void onMemberLeft(final MemberImpl member) {
        // postpone notifying calls since real response may arrive in the mean time.
        Runnable task = new Runnable() {
            @Override
            public void run() {
                Iterator<Invocation> iterator = invocations.values().iterator();
                while (iterator.hasNext()) {
                    Invocation invocation = iterator.next();
                    if (!isCallTarget(invocation, member)) {
                        continue;
                    }

                    System.out.println("OnMemberLeft for invocation: " + invocation);
                    iterator.remove();
                    invocation.notify(new MemberLeftException(member));
                }
            }
        };
        InternalExecutionService executionService = nodeEngine.getExecutionService();
        executionService.schedule(task, SCHEDULE_DELAY, TimeUnit.MILLISECONDS);
    }

    private boolean isCallTarget(Invocation invocation, MemberImpl leftMember) {
        MemberImpl targetMember = invocation.invTargetMember;
        if (targetMember == null) {
            Address invTarget = invocation.invTarget;
            return leftMember.getAddress().equals(invTarget);
        } else {
            return leftMember.getUuid().equals(targetMember.getUuid());
        }
    }

    public void reset() {
        for (Invocation invocation : invocations.values()) {
            try {
                invocation.notify(new MemberLeftException());
            } catch (Throwable e) {
                logger.warning(invocation + " could not be notified with reset message -> " + e.getMessage());
            }
        }
        invocations.clear();
    }

    public void shutdown() {
        for (Invocation invocation : invocations.values()) {
            try {
                invocation.notify(new HazelcastInstanceNotActiveException());
            } catch (Throwable e) {
                logger.warning(invocation + " could not be notified with shutdown message -> " + e.getMessage());
            }
        }
        invocations.clear();
    }
}
