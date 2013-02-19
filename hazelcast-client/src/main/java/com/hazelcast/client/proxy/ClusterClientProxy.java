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

package com.hazelcast.client.proxy;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.proxy.listener.ListenerThread;
import com.hazelcast.client.proxy.listener.MembershipLRH;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Protocol;
import com.hazelcast.nio.protocol.Command;

import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterClientProxy implements Cluster {
    final ProxyHelper proxyHelper;
    final private HazelcastClient client;

    public ClusterClientProxy(HazelcastClient client) {
        this.client = client;
        proxyHelper = new ProxyHelper(client.getSerializationService(), client.getConnectionPool());
    }

    Map<MembershipListener, ListenerThread> listenerMap = new ConcurrentHashMap<MembershipListener, ListenerThread>();

    public void addMembershipListener(MembershipListener listener) {
        Protocol request = proxyHelper.createProtocol(Command.MEMBERLISTEN, null, null);
        ListenerThread thread = proxyHelper.createAListenerThread("hz.client.membershipListener.",
                client, request, new MembershipLRH(listener, this));
        listenerMap.put(listener, thread);
        thread.start();
    }

    public void removeMembershipListener(MembershipListener listener) {
        ListenerThread thread = listenerMap.remove(listener);
        thread.stopListening();
    }

    public Set<Member> getMembers() {
        Protocol protocol = proxyHelper.doCommand(Command.MEMBERS, null);
        Set<Member> members = new HashSet<Member>();
        for (int i = 0; i < protocol.args.length; ) {
            String hostname = protocol.args[i++];
            int port = Integer.valueOf(protocol.args[i++]);
            try {
                Member member = new MemberImpl(new Address(hostname, port), false);
                members.add(member);
            } catch (UnknownHostException e) {
            }
        }
        return members;
    }

    public Member getLocalMember() {
        throw new UnsupportedOperationException();
    }

    public long getClusterTime() {
        Protocol protocol = proxyHelper.doCommand(Command.CLUSTERTIME, null);
        long time = Long.valueOf(protocol.args[0]);
        return time;
    }

    @Override
    public String toString() {
        Set<Member> members = getMembers();
        StringBuffer sb = new StringBuffer("Cluster [");
        if (members != null) {
            sb.append(members.size());
            sb.append("] {");
            for (Member member : members) {
                sb.append("\n\t").append(member);
            }
        }
        sb.append("\n}\n");
        return sb.toString();
    }
}
