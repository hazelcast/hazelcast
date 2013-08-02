package com.hazelcast.core;

/**
 * The InitializingMembershipListener is a {@link MembershipListener} that will first receives a
 * {@link InitialMembershipEvent} when it is registered so it immediately knows which members are available. After
 * that event has been received, it will receive the normal MembershipEvents.
 *
 * When the InitializingMembershipListener already is registered on a {@link Cluster} and is registered again on the same
 * Cluster instance, it will not receive an additional MembershipInitializeEvent. So this is a once only event.
 *
 * @author Peter Veentjer.
 * @see Cluster#addMembershipListener(MembershipListener)
 * @see com.hazelcast.core.MembershipEvent#getMembers()
 */
public interface InitialMembershipListener extends MembershipListener {

    /**
     * Is called when this listener is registered.
     *
     * @param event the MembershipInitializeEvent
     */
    void init(InitialMembershipEvent event);
}
