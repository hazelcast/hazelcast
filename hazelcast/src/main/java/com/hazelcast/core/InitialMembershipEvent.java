package com.hazelcast.core;

import java.util.EventObject;
import java.util.Set;

/**
 * A event that is send when a {@link InitialMembershipListener} registers itself on a {@link Cluster}. For more
 * information see the {@link InitialMembershipListener}.
 *
 * @author Peter Veentjer
 * @see InitialMembershipListener
 * @see MembershipListener
 * @see MembershipEvent
 */
public class InitialMembershipEvent extends EventObject {

    private static final long serialVersionUID = -2010865371829087371L;

    private final Set<Member> members;

    public InitialMembershipEvent(Cluster cluster, Set<Member> members) {
        super(cluster);
        this.members = members;
    }

    /**
     * Returns an immutable set of ordered members at the moment this {@link InitialMembershipListener} is
     * registered. See {@link com.hazelcast.core.Cluster#getMembers()} for more information.
     *
     * @return a set of members.
     */
    public Set<Member> getMembers() {
        return members;
    }

    /**
     * Returns the cluster of the event.
     *
     * @return the cluster of the event.
     */
    public Cluster getCluster() {
        return (Cluster) getSource();
    }

    @Override
    public String toString() {
        return "MembershipInitializeEvent {" + members + "}";
    }
}
