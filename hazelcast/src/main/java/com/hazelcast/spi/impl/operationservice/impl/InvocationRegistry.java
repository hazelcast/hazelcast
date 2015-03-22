package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.ReplicaErrorLogger;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.InternalExecutionService;
import com.hazelcast.spi.impl.operationservice.impl.responses.BackupResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.CallTimeoutResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.ErrorResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.Response;
import com.hazelcast.util.EmptyStatement;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.inspectOutputMemoryError;
import static com.hazelcast.spi.OperationAccessor.setCallId;

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
 * - apply back pressure when there is no room for more invocations.
 */
public class InvocationRegistry {
    private static final long SCHEDULE_DELAY = 1111;
    private static final int INITIAL_CAPACITY = 1000;
    private static final float LOAD_FACTOR = 0.75f;
    private static final int DELAY_MILLIS = 1000;

    private final long backupTimeoutMillis;
    private final AtomicLong callIdGen = new AtomicLong(0);
    private final AtomicLong retiredCounter = new AtomicLong();
    private final ConcurrentMap<Long, Invocation> invocations;
    private final OperationServiceImpl operationService;
    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;
    private final InspectionThread inspectionThread;
    private final int maxConcurrentInvocations;

    public InvocationRegistry(OperationServiceImpl operationService, int concurrencyLevel, long backupTimeoutMillis) {
        this.operationService = operationService;
        this.nodeEngine = operationService.nodeEngine;
        this.logger = operationService.logger;
        this.backupTimeoutMillis = backupTimeoutMillis;
        //todo: this number need to be retrieved from GroupProperties.
        this.maxConcurrentInvocations = 1000;
        this.invocations = new ConcurrentHashMap<Long, Invocation>(INITIAL_CAPACITY, LOAD_FACTOR, concurrencyLevel);
        this.inspectionThread = new InspectionThread();
        inspectionThread.start();
    }

    public long getLastCallId() {
        return callIdGen.get();
    }

    /**
     * Registers an invocation.
     * <p/>
     * It depends on the invocation if the registration actually happens. See {@link #skipRegistration(Invocation)}
     *
     * @param invocation the invocation to register.
     */
    public void register(Invocation invocation) {
        Operation op = invocation.op;

        // we need to determine the call id even for calls that can be skipped. Because these invocations need to be
        // controlled in number.
        long nextCallId = nextCallId(op.isUrgent());

        if (skipRegistration(invocation)) {
            return;
        }

        // if the invocation was already registered, deregister it first.
        long previousCallId = op.getCallId();
        if (previousCallId != 0) {
            invocations.remove(previousCallId);
        }

        invocations.put(nextCallId, invocation);
        setCallId(invocation.op, nextCallId);
    }

    /**
     * Gets the next call id.
     * <p/>
     * If the maximum number of pending invocation has been reached, back-pressure is applied. So we are going to do some waiting.
     * Currently this isn't configurable and it doesn't respect the timeout of the invocation.
     *
     * The actual implementation isn't very strict. It can happen more
     *
     * @param priority if true, then no back-pressure is applied. You will immediately get your next sequence-id.
     * @return the next call id.
     */
    private long nextCallId(boolean priority) {
        if (priority) {
            return callIdGen.incrementAndGet();
        }

        boolean restoreInterrupt = false;
        try {
            int delayMs = 1;
            for (; ; ) {
                boolean hasSpace = callIdGen.get() - retiredCounter.get() < maxConcurrentInvocations;

                if (hasSpace) {
                    break;
                }

                //back-pressure
                if (delayMs > 500) {
                    delayMs = 500;
                }

                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException e) {
                    restoreInterrupt = true;
                }
                delayMs *= 2;
            }

            return callIdGen.incrementAndGet();
        } finally {
            if (restoreInterrupt) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Not every call needs to be registered. A call that is local and has no backups, doesn't need to be registered
     * since there will not be a remote machine sending a response back to the invocation.
     *
     * @param invocation
     * @return true if registration is required, false otherwise.
     */
    private boolean skipRegistration(Invocation invocation) {
        if (invocation.remote) {
            return false;
        }

        if (invocation.op instanceof BackupAwareOperation) {
            return false;
        }

        return true;
    }

    /**
     * Deregisters an invocation.
     * <p/>
     * If the invocation registration was skipped, the call is ignored.
     *
     * @param invocation the Invocation to deregister.
     */
    public void deregister(Invocation invocation) {
        retiredCounter.incrementAndGet();

        long callId = invocation.op.getCallId();

        // if an invocation skipped registration (so call id is 0) we don't need to deregister it.
        if (callId == 0) {
            return;
        }

        if (invocations.remove(callId) == null) {
            throw new IllegalArgumentException("Invocation " + invocation + " was not deregistered successfully since no " +
                    "invocation was found.");
        }
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

    /**
     * Notifies the invocation that a Response is available.
     *
     * @param response
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
        try {
            Invocation invocation = invocations.get(callId);
            if (invocation == null) {
                return;
            }

            invocation.notifyOneBackupComplete();
        } catch (Exception e) {
            ReplicaErrorLogger.log(e, logger);
        }
    }

    private void notifyErrorResponse(ErrorResponse response) {
        Invocation invocation = invocations.get(response.getCallId());

        if (invocation == null) {
            if (nodeEngine.isActive()) {
                logger.warning("No Invocation found for response: " + response);
            }
            return;
        }

        invocation.notifyErrorResponse(response.getCause());
    }

    private void notifyNormalResponse(NormalResponse response) {
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
        Invocation invocation = invocations.get(response.getCallId());

        if (invocation == null) {
            if (nodeEngine.isActive()) {
                logger.warning("No Invocation found for response: " + response);
            }
            return;
        }
        invocation.notifyCallTimeoutResponse();
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
        inspectionThread.shutdown();

        for (Invocation invocation : invocations.values()) {
            try {
                invocation.notify(new HazelcastInstanceNotActiveException());
            } catch (Throwable e) {
                logger.warning(invocation + " could not be notified with shutdown message -> " + e.getMessage());
            }
        }
        invocations.clear();
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
     */
    class InspectionThread extends Thread {

        private volatile boolean shutdown;

        InspectionThread() {
            super(operationService.node.getHazelcastThreadGroup().getThreadNamePrefix("InspectInvocationsThread"));
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
                operationService.logger.severe("Failed to run", t);
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

            for (Invocation invocation : invocations.values()) {
                if (shutdown) {
                    return;
                }

                try {
                    invocation.notifyInvocationTimeout();
                } catch (Throwable t) {
                    inspectOutputMemoryError(t);
                    logger.severe("Failed to handle operation timeout of invocation:" + invocation, t);
                }

                try {
                    invocation.checkBackupTimeout(backupTimeoutMillis);
                } catch (Throwable t) {
                    inspectOutputMemoryError(t);
                    logger.severe("Failed to handle backup timeout of invocation:" + invocation, t);
                }
            }
        }
    }
}
