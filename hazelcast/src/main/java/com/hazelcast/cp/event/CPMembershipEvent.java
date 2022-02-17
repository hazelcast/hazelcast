/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.event;

import com.hazelcast.cp.CPMember;

/**
 * CPMembershipEvent is published when a CP member is added to
 * or removed from the CP Subsystem.
 *
 * @see CPMembershipListener
 * @since 4.1
 */
public interface CPMembershipEvent {

    /**
     * Member ADDED event ID.
     */
    byte MEMBER_ADDED = 1;

    /**
     * Member REMOVED event ID.
     */
    byte MEMBER_REMOVED = 2;

    /**
     * Membership event type.
     */
    enum EventType {
        /**
         * Event type fired when a new CP member is added to the CP Subsystem.
         */
        ADDED(MEMBER_ADDED),
        /**
         * Event type fired when a CP member is removed from the CP Subsystem.
         */
        REMOVED(MEMBER_REMOVED);

        private final byte id;

        EventType(byte id) {
            this.id = id;
        }

        public byte id() {
            return id;
        }
    }

    /**
     * Returns the CPMember that is added to
     * or removed from CP Subsystem.
     *
     * @return the CP member
     */
    CPMember getMember();

    /**
     * Returns the type of membership change.
     *
     * @return membership event type
     */
    EventType getType();
}
