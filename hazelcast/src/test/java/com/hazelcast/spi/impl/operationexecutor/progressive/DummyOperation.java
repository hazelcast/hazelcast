package com.hazelcast.spi.impl.operationexecutor.progressive;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;

public class DummyOperation<E> extends AbstractOperation {
    private final Object NO_RESPONSE = new Object();

    private int durationMs;
    protected volatile Thread executingThread = null;
    private volatile Object response = NO_RESPONSE;
    private volatile Throwable throwable;

    public DummyOperation(int partitionId) {
        setPartitionId(partitionId);
    }

    public Thread getExecutingThread() {
        return executingThread;
    }

    public DummyOperation() {
    }

    public DummyOperation durationMs(int durationMs) {
        this.durationMs = durationMs;
        return this;
    }

    @Override
    public final void run() throws Exception {
        executingThread = Thread.currentThread();

        try {
            Thread.sleep(durationMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        try {
            response = call();
        } catch (Throwable t) {
            throwable = t;
        }

        synchronized (this) {
            notifyAll();
        }
    }

    public E call() throws Throwable {
        return null;
    }

    @Override
    public Object getResponse() {
        return response;
    }

    /**
     * Gets the result of the operation or blocks.
     * <p/>
     * Fails with GetTimeoutException when the call times out. Any exception thrown in the #call method will be propagated.
     *
     * @param timeoutMs
     * @return the result
     * @throws Throwable any exception thrown in the call method is propagated.
     */
    public E get(long timeoutMs) throws Throwable {
        if (response == NO_RESPONSE && throwable == null) {
            synchronized (this) {
                for (; ; ) {
                    if (response != NO_RESPONSE) {
                        break;
                    }

                    if (throwable != null) {
                        break;
                    }

                    if (timeoutMs <= 0) {
                        throw new GetTimeoutException("Operation failed to complete within timeout, op=" + this);
                    }

                    long startMs = System.currentTimeMillis();
                    wait(timeoutMs);
                    long durationMs = System.currentTimeMillis() - startMs;
                    timeoutMs -= durationMs;
                }
            }
        }

        if (response != NO_RESPONSE) {
            return (E) response;
        }

        ExceptionUtil.fixRemoteStackTrace(throwable, Thread.currentThread().getStackTrace());
        throw throwable;
    }

    /**
     * Gets the result of the operation or blocks.
     *
     * @return the result
     * @throws Throwable any exception thrown in the call method is propagated.
     */
    public E get() throws Throwable {
        return get(Long.MAX_VALUE);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(durationMs);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        durationMs = in.readInt();
    }
}
