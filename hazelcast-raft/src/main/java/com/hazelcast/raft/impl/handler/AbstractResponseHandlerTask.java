package com.hazelcast.raft.impl.handler;

import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.task.RaftNodeAwareTask;

/**
 * Base class for response handler tasks.
 * Subclasses must implement {@link #handleResponse()}.
 * <p>
 * If {@link #senderEndpoint()} is not a known endpoint, then response is ignored.
 */
public abstract class AbstractResponseHandlerTask extends RaftNodeAwareTask {

    public AbstractResponseHandlerTask(RaftNodeImpl raftNode) {
        super(raftNode);
    }

    @Override
    protected final void innerRun() {
        RaftEndpoint sender = senderEndpoint();
        if (!raftNode.state().isKnownEndpoint(sender)) {
            logger.warning("Won't run, since " + sender + " is unknown to us");
            return;
        }

        handleResponse();
    }

    protected abstract void handleResponse();

    protected abstract RaftEndpoint senderEndpoint();

}
