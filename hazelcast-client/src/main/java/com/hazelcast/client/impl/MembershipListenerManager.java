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

package com.hazelcast.client.impl;

import com.hazelcast.client.ClientConfig;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.Packet;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.hazelcast.client.IOUtil.toObject;

public class MembershipListenerManager {

    final private List<MembershipListener> memberShipListeners = new CopyOnWriteArrayList<MembershipListener>();
    final private HazelcastClient client;

    public MembershipListenerManager(HazelcastClient client) {
        this.client = client;
        final ClientConfig clientConfig = client.getClientConfig();
        if (clientConfig != null) {
            for (Object listener : clientConfig.getListeners()) {
                if (listener instanceof MembershipListener) {
                    registerListener((MembershipListener) listener);
                }
            }
        }
    }

    public void registerListener(MembershipListener listener) {
        this.memberShipListeners.add(listener);
    }

    public void removeListener(MembershipListener listener) {
        this.memberShipListeners.remove(listener);
    }

    public boolean noListenerRegistered() {
        return memberShipListeners.isEmpty();
    }

    public void notifyListeners(Packet packet) {
        if (memberShipListeners.size() > 0) {
            Member member = (Member) toObject(packet.getKey());
            Integer type = (Integer) toObject(packet.getValue());
            MembershipEvent event = new MembershipEvent(client.getCluster(), member, type);
            if (type.equals(MembershipEvent.MEMBER_ADDED)) {
                for (MembershipListener membershipListener : memberShipListeners) {
                    membershipListener.memberAdded(event);
                }
            } else {
                for (MembershipListener membershipListener : memberShipListeners) {
                    membershipListener.memberRemoved(event);
                }
            }
        }
    }
}
