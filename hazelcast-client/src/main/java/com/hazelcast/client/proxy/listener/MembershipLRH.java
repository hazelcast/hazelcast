/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.client.proxy.listener;

import com.hazelcast.client.connection.Connection;
import com.hazelcast.client.proxy.ClusterClientProxy;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.deprecated.nio.Protocol;
import com.hazelcast.nio.serialization.SerializationService;

public class MembershipLRH implements ListenerResponseHandler {

    final MembershipListener listener;
    final ClusterClientProxy clusterClientProxy;

    public MembershipLRH(MembershipListener listener, ClusterClientProxy clusterClientProxy) {
        this.clusterClientProxy = clusterClientProxy;
        this.listener = listener;
    }

    public void handleResponse(Protocol response, SerializationService ss) throws Exception {
        String eventType = response.args[0];
        int eventTypeId;
        if ("ADDED".equalsIgnoreCase(eventType))
            eventTypeId = MembershipEvent.MEMBER_ADDED;
        else
            eventTypeId = MembershipEvent.MEMBER_REMOVED;
        String host = response.args[1];
        int port = Integer.valueOf(response.args[2]);
        Address address = new Address(host, port);
        Member member = new MemberImpl(address, false);
        MembershipEvent event = new MembershipEvent(member, eventTypeId);
        switch (eventTypeId) {
            case MembershipEvent.MEMBER_ADDED:
                listener.memberAdded(event);
                break;
            case MembershipEvent.MEMBER_REMOVED:
                listener.memberRemoved(event);
                break;
        }
    }

    public void onError(Connection connection, Exception e) {
        listener.memberRemoved(new MembershipEvent(new MemberImpl(connection.getAddress(), false), MembershipEvent.MEMBER_REMOVED));
        clusterClientProxy.addMembershipListener(listener);
    }
}
