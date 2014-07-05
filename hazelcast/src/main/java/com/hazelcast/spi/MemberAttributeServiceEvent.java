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

package com.hazelcast.spi;

import com.hazelcast.cluster.MemberAttributeOperationType;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.instance.MemberImpl;

/**
 * This service event is fired to inform services about a change in a member's attributes collection
 */
public class MemberAttributeServiceEvent extends MemberAttributeEvent {

    public MemberAttributeServiceEvent() {
    }

    public MemberAttributeServiceEvent(Cluster cluster, MemberImpl member, MemberAttributeOperationType operationType,
                                       String key, Object value) {
        super(cluster, member, operationType, key, value);
    }

}
