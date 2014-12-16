/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.core;

/**
 * The InitializingMembershipListener is a {@link MembershipListener} that first receives a
 * {@link InitialMembershipEvent} when it is registered so it immediately knows which members are available. After
 * that event has been received, it will receive the normal MembershipEvents.
 *
 * When the InitializingMembershipListener already is registered on a {@link Cluster} and is registered again on the same
 * Cluster instance, it will not receive an additional MembershipInitializeEvent. This is a once only event.
 *
 * @author Peter Veentjer.
 * @see Cluster#addMembershipListener(MembershipListener)
 * @see com.hazelcast.core.MembershipEvent#getMembers()
 */
public interface InitialMembershipListener extends MembershipListener {

    /**
     * Called when this listener is registered.
     *
     * @param event the MembershipInitializeEvent received when the listener is registered
     */
    void init(InitialMembershipEvent event);
}
