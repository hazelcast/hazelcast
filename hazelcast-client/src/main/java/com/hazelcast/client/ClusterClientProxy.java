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

package com.hazelcast.client;

import com.hazelcast.client.impl.CollectionWrapper;
import com.hazelcast.core.*;
import com.hazelcast.impl.ClusterOperation;
import com.hazelcast.impl.FactoryImpl;

import java.util.*;

import static com.hazelcast.client.ProxyHelper.check;

public class ClusterClientProxy implements Cluster {
    final ProxyHelper proxyHelper;
    final private HazelcastClient client;

    public ClusterClientProxy(HazelcastClient client) {
        this.client = client;
        proxyHelper = new ProxyHelper("", client);
    }

    public Collection<Instance> getInstances() {
        Object[] instances = (Object[]) proxyHelper.doOp(ClusterOperation.GET_INSTANCES, null, null);
        List<Instance> list = new ArrayList<Instance>();
        if (instances != null) {
            for (int i = 0; i < instances.length; i++) {
                if (instances[i] instanceof FactoryImpl.ProxyKey) {
                    FactoryImpl.ProxyKey proxyKey = (FactoryImpl.ProxyKey) instances[i];
                    list.add((Instance) client.getClientProxy(proxyKey.getKey()));
                } else {
                    list.add((Instance) client.getClientProxy(instances[i]));
                }
            }
        }
        return list;
    }

    public void addMembershipListener(MembershipListener listener) {
        check(listener);
        if (client.listenerManager.membershipListenerManager.noMembershipListenerRegistered()) {
            Packet request = proxyHelper.createRequestPacket(ClusterOperation.CLIENT_ADD_MEMBERSHIP_LISTENER, null, null);
            Call c = proxyHelper.createCall(request);
            client.listenerManager.addListenerCall(c);
            proxyHelper.doCall(c);
        }
        client.listenerManager.membershipListenerManager.registerMembershipListener(listener);
    }

    public void removeMembershipListener(MembershipListener listener) {
        client.listenerManager.membershipListenerManager.removeMembershipListener(listener);
    }

    public Set<Member> getMembers() {
        CollectionWrapper<Member> cw = (CollectionWrapper<Member>) proxyHelper.doOp(ClusterOperation.GET_MEMBERS, null, null);
        return new LinkedHashSet<Member>(cw.getKeys());
    }

    public Member getLocalMember() {
        throw new UnsupportedOperationException();
    }

    public long getClusterTime() {
        return (Long) proxyHelper.doOp(ClusterOperation.GET_CLUSTER_TIME, null, null);
    }

    public boolean authenticate(String groupName, String groupPassword) {
        Object result = proxyHelper.doOp(ClusterOperation.CLIENT_AUTHENTICATE, groupName, groupPassword);
        return (result == null) ? false : (Boolean) result;
    }

    public void addInstanceListener(InstanceListener listener) {
        check(listener);
//        if(client.listenerManager.noInstanceListenerRegistered()){
        if (client.listenerManager.instanceListenerManager.noInstanceListenerRegistered()) {
            Packet request = proxyHelper.createRequestPacket(ClusterOperation.CLIENT_ADD_INSTANCE_LISTENER, null, null);
            Call c = proxyHelper.createCall(request);
            client.listenerManager.addListenerCall(c);
            proxyHelper.doCall(c);
        }
//	    client.listenerManager.registerInstanceListener(listener);
        client.listenerManager.instanceListenerManager.registerInstanceListener(listener);
    }

    public void removeInstanceListener(InstanceListener instanceListener) {
        check(instanceListener);
//		client.listenerManager.removeInstanceListener(instanceListener);
        client.listenerManager.instanceListenerManager.removeInstanceListener(instanceListener);
    }
}
