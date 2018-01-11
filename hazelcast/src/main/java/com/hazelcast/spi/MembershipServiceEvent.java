/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi;

import com.hazelcast.core.MembershipEvent;
import com.hazelcast.instance.MemberImpl;

/**
 * Membership event fired when a new member is added
 * to the cluster and/or when a member leaves the cluster.
 *
 * @see com.hazelcast.spi.MembershipAwareService
 */
public class MembershipServiceEvent extends MembershipEvent {

    public MembershipServiceEvent(MembershipEvent e) {
        super(e.getCluster(), e.getMember(), e.getEventType(), e.getMembers());
    }

    /**
     * Returns the removed or added member.
     *
     * @return member which is removed/added
     */
    public MemberImpl getMember() {
        return (MemberImpl) super.getMember();
    }
}
