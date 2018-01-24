package com.hazelcast.raft.exception;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.raft.impl.RaftEndpoint;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Base exception for Raft related failures.
 * <p>
 * This exception can include the known leader of a Raft group when & where it's thrown.
 * Leader endpoint can be accessed by {@link #getLeader()}, if available.
 */
public class RaftException extends HazelcastException {

    private transient RaftEndpoint leader;

    public RaftException(RaftEndpoint leader) {
        this.leader = leader;
    }

    public RaftException(String message, RaftEndpoint leader) {
        super(message);
        this.leader = leader;
    }

    /**
     * Returns the leader endpoint of related Raft group, if known/available
     * by the time this exception is thrown.
     */
    public RaftEndpoint getLeader() {
        return leader;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();

        if (leader == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeObject(leader);
        }
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        if (in.readBoolean()) {
            leader = (RaftEndpoint) in.readObject();
        }
    }
}
