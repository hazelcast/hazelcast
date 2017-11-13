package com.hazelcast.raft.impl.state;

import com.hazelcast.raft.impl.RaftEndpoint;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;

/**
 * Immutable container for members of a Raft group with an index identifying
 * membership change's position in the Raft log.
 */
public class RaftGroupMembers {

    private final long index;

    private final Collection<RaftEndpoint> members;

    private final Collection<RaftEndpoint> remoteMembers;

    public RaftGroupMembers(long index, Collection<RaftEndpoint> endpoints, RaftEndpoint localEndpoint) {
        this.index = index;
        this.members = unmodifiableSet(new LinkedHashSet<RaftEndpoint>(endpoints));
        Set<RaftEndpoint> remoteMembers = new LinkedHashSet<RaftEndpoint>(endpoints);
        remoteMembers.remove(localEndpoint);
        this.remoteMembers = unmodifiableSet(remoteMembers);
    }

    /**
     * Returns the position of the membership change that leads to formation of this group.
     */
    public long index() {
        return index;
    }

    /**
     * Return all members in this group.
     *
     * @see #remoteMembers()
     */
    public Collection<RaftEndpoint> members() {
        return members;
    }

    /**
     * Returns remote members in this group, excluding the local member.
     */
    public Collection<RaftEndpoint> remoteMembers() {
        return remoteMembers;
    }

    /**
     * Returns the number of members in this group.
     */
    public int memberCount() {
        return members.size();
    }

    /**
     * Returns the majority for this group.
     */
    public int majority() {
        return members.size() / 2 + 1;
    }

    /**
     * Returns true if the endpoint is a member of this group, false otherwise.
     */
    public boolean isKnownEndpoint(RaftEndpoint endpoint) {
        return members.contains(endpoint);
    }

    @Override
    public String toString() {
        return "RaftGroupMembers{" + "index=" + index + ", members=" + members + '}';
    }

}
