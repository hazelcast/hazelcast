package com.hazelcast.raft.impl.handler;

import com.hazelcast.raft.RaftMember;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.task.RaftNodeStatusAwareTask;

/**
 * Base class for response handler tasks.
 * Subclasses must implement {@link #handleResponse()}.
 * <p>
 * If {@link #sender()} is not a known member, then response is ignored.
 */
public abstract class AbstractResponseHandlerTask extends RaftNodeStatusAwareTask {

    public AbstractResponseHandlerTask(RaftNodeImpl raftNode) {
        super(raftNode);
    }

    @Override
    protected final void innerRun() {
        RaftMember sender = sender();
        if (!raftNode.state().isKnownMember(sender)) {
            logger.warning("Won't run, since " + sender + " is unknown to us");
            return;
        }

        handleResponse();
    }

    protected abstract void handleResponse();

    protected abstract RaftMember sender();

}
