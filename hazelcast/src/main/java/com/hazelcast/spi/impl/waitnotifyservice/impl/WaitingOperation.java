package com.hazelcast.spi.impl.waitnotifyservice.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.WaitSupport;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.spi.impl.CallTimeoutResponse;
import com.hazelcast.spi.impl.InternalOperationService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.Clock;

import java.util.Queue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static com.hazelcast.util.EmptyStatement.ignore;

class WaitingOperation extends AbstractOperation implements Delayed, PartitionAwareOperation {
    final Queue<WaitingOperation> queue;
    final Operation op;
    final WaitSupport waitSupport;
    final long expirationTime;
    volatile boolean valid = true;
    volatile Object cancelResponse;

    WaitingOperation(Queue<WaitingOperation> queue, WaitSupport waitSupport) {
        this.op = (Operation) waitSupport;
        this.waitSupport = waitSupport;
        this.queue = queue;
        this.expirationTime = getExpirationTime(waitSupport);
        this.setPartitionId(op.getPartitionId());
    }

    private long getExpirationTime(WaitSupport waitSupport) {
        long waitTimeout = waitSupport.getWaitTimeout();
        if (waitTimeout < 0) {
            return -1;
        }
        long expirationTime = Clock.currentTimeMillis() + waitTimeout;
        if (expirationTime < 0) {
            return -1;
        }
        return expirationTime;
    }

    public Operation getOperation() {
        return op;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

    public boolean isValid() {
        return valid;
    }

    public boolean needsInvalidation() {
        return isExpired() || isCancelled() || isCallTimedOut();
    }

    public boolean isExpired() {
        return expirationTime > 0 && Clock.currentTimeMillis() >= expirationTime;
    }

    public boolean isCancelled() {
        return cancelResponse != null;
    }

    public boolean isCallTimedOut() {
        final NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        InternalOperationService operationService = nodeEngine.getOperationService();
        if (operationService.isCallTimedOut(op)) {
            cancel(new CallTimeoutResponse(op.getCallId(), op.isUrgent()));
            return true;
        }
        return false;
    }

    public boolean shouldWait() {
        return waitSupport.shouldWait();
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(expirationTime - Clock.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed other) {
        // compare zero ONLY if same object
        if (other == this) {
            return 0;
        }
        long d = (getDelay(TimeUnit.NANOSECONDS)
                - other.getDelay(TimeUnit.NANOSECONDS));
        return (d == 0) ? 0 : ((d < 0) ? -1 : 1);
    }

    @Override
    public void run() throws Exception {
        if (!valid) {
            return;
        }

        boolean expired = isExpired();
        boolean cancelled = isCancelled();
        if (!expired && !cancelled) {
            return;
        }

        if (!queue.remove(this)) {
            return;
        }

        valid = false;
        if (expired) {
            waitSupport.onWaitExpire();
        } else {
            op.getResponseHandler().sendResponse(cancelResponse);
        }
    }

    //If you don't think instances of this class will ever be inserted into a HashMap/HashTable,
    // the recommended hashCode implementation to use is:
    @Override
    public int hashCode() {
        assert false : "hashCode not designed";
        return 42;
        // any arbitrary constant will do
    }

    @Override
    // use object.equals
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public void logError(Throwable e) {
        final ILogger logger = getLogger();
        if (e instanceof RetryableException) {
            logger.warning("Op: " + op + ", " + e.getClass().getName() + ": " + e.getMessage());
        } else if (e instanceof OutOfMemoryError) {
            try {
                logger.log(Level.SEVERE, e.getMessage(), e);
            } catch (Throwable ignored) {
                ignore(ignored);
            }
        } else {
            logger.severe("Op: " + op + ", Error: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public String getServiceName() {
        return op.getServiceName();
    }

    public void onExpire() {
        waitSupport.onWaitExpire();
    }

    public void cancel(Object error) {
        this.cancelResponse = error;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("WaitingOp");
        sb.append("{op=").append(op);
        sb.append(", expirationTime=").append(expirationTime);
        sb.append(", valid=").append(valid);
        sb.append('}');
        return sb.toString();
    }
}
