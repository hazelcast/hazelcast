package com.hazelcast.internal.query.worker;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;

public abstract class AbstractWorker<T extends WorkerTask> implements Runnable {
    // TODO: MPSC
    private final LinkedBlockingDeque<WorkerTask> queue = new LinkedBlockingDeque<>();

    private final CountDownLatch startLatch = new CountDownLatch(1);

    private boolean stop;

    public void awaitStart() {
        try {
            startLatch.await();
        }
        catch (InterruptedException ignore) {
            // TODO: Handle.
        }
    }

    public void offer(T task) {
        queue.offer(task);
    }

    public void stop() {
        queue.offerFirst(StopWorkerTask.INSTANCE);
    }

    @Override
    public void run() {
        startLatch.countDown();

        while (!stop) {
            try {
                WorkerTask nextTask = queue.take();

                if (nextTask instanceof StopWorkerTask) {
                    try {
                        // TODO
                    }
                    finally {
                        stop = true;
                    }
                }
                else
                    executeTask((T)nextTask);
            }
            catch (InterruptedException e) {
                // TODO: Handle interrupt.
            }
            catch (Exception e) {
                // TODO: Handle generic exception?
            }
        }
    }

    protected abstract void executeTask(T task);

    protected abstract void onStop();
}
