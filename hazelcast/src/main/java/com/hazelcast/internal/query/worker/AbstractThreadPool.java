package com.hazelcast.internal.query.worker;

public abstract class AbstractThreadPool<T extends AbstractWorker> {
    private final String threadPrexix;
    private final int threadCnt;

    protected final AbstractWorker[] workers;

    public AbstractThreadPool(String threadPrexix, int threadCnt) {
        this.threadPrexix = threadPrexix;
        this.threadCnt = threadCnt;

        workers = new AbstractWorker[threadCnt];
    }

    public void start() {
        for (int i = 0; i < threadCnt; i++) {
            T worker = createWorker(i);

            Thread thread = new Thread(worker);

            thread.setName(threadPrexix + "-" + i);
            thread.start();

            workers[i] = worker;
        }

        for (AbstractWorker worker : workers)
            worker.awaitStart();
    }

    public void shutdown() {
        for (AbstractWorker worker : workers)
            worker.stop();
    }

    protected int getThreadCount() {
        return workers.length;
    }

    protected T getWorker(int idx) {
        return (T)workers[idx];
    }

    protected T[] getWorkers() {
        return (T[])workers;
    }

    protected abstract T createWorker(int idx);
}
