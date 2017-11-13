package com.hazelcast.raft.impl.task;

import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.impl.RaftNodeImpl;

/**
 * Base class for tasks need to know current {@link RaftNodeImpl}. If this not is terminated or stepped down,
 * task will be skipped.
 * <p>
 * Subclasses must implement {@link #innerRun()} method.
 */
public abstract class RaftNodeAwareTask implements Runnable {

    protected final RaftNodeImpl raftNode;
    protected final ILogger logger;

    protected RaftNodeAwareTask(RaftNodeImpl raftNode) {
        this.raftNode = raftNode;
        this.logger = raftNode.getLogger(getClass());
    }

    @Override
    public final void run() {
        if (raftNode.isTerminatedOrSteppedDown()) {
            logger.fine("Won't run, since raft node is terminated");
            return;
        }

        innerRun();
    }

    protected abstract void innerRun();

}
