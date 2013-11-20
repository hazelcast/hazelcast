package com.hazelcast.spi.impl;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class BackupSynchronizer {

    private volatile int backupCount=-1;
    private final AtomicInteger completedBackupCount = new AtomicInteger();
    private final AtomicReference<Runnable> taskRef = new AtomicReference<Runnable>();

    public int getBackupCount() {
        return backupCount;
    }

    public int getCompletedBackupCount() {
        return completedBackupCount.get();
    }

    public boolean isFinished(){
        if(backupCount==-1){
            return false;
        }

        return backupCount==completedBackupCount.get();
    }

    public void signalBackupComplete() {
        int b = completedBackupCount.incrementAndGet();

        //if the backups have not yet all returned we are done.
        if (backupCount - b != 0) {
            return;
        }

        //if there is no registered task, we are done.
        if (taskRef.get() == null) {
            return;
        }

        //so there is a registered task and all backups are returned,
        //try to remove the task. If that was done, the task can be executed.
        Runnable task = taskRef.getAndSet(null);
        if (task != null) {
            task.run();
        }
    }

    public void onBackupComplete(int backupCount, Runnable task) {
        this.backupCount =backupCount;

        if (task == null) {
            throw new NullPointerException();
        }

        //if all backups are complete, the task can be executed immediately.
        if (isFinished()) {
            task.run();
            return;
        }

        //se the task
        taskRef.set(task);

        //if the backups have not completed yet, then we are done.
        if (!isFinished()) {
            return;
        }

        //so the backs are complete, try to get back the task that has been set.
        if (taskRef.compareAndSet(task, null)) {
            task.run();
        }
    }
}
