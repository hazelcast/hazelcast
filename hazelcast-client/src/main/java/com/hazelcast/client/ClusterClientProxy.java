/*
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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

import com.hazelcast.core.Instance;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.core.Member;
import com.hazelcast.impl.ClusterOperation;
import com.hazelcast.client.impl.CollectionWrapper;

import java.util.*;

public class ClusterClientProxy implements ClientProxy, Cluster {
    final ProxyHelper proxyHelper;
	final private HazelcastClient client;

	public ClusterClientProxy(HazelcastClient client) {
		this.client = client;
		proxyHelper = new ProxyHelper("", client);
	}


    public void setOutRunnable(OutRunnable out) {
        proxyHelper.setOutRunnable(out);
    }

    public Collection<Instance> getInstances() {
        Object[] instances = (Object[])proxyHelper.doOp(ClusterOperation.GET_INSTANCES, null, null);
        List<Instance> list = new ArrayList<Instance>();
        for(int i=0;i<instances.length;i++){
            list.add((Instance)client.getClientProxy((String)instances[i]));
        }
        return list;
    }

    public void addMembershipListener(MembershipListener listener) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void removeMembershipListener(MembershipListener listener) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public Set<Member> getMembers() {
        CollectionWrapper<Member> cw = (CollectionWrapper<Member>)proxyHelper.doOp(ClusterOperation.GET_MEMBERS, null, null);
        return new HashSet<Member>(cw.getKeys());
    }

    public Member getLocalMember() {
        throw new UnsupportedOperationException();
    }

    public long getClusterTime() {
        return (Long)proxyHelper.doOp(ClusterOperation.GET_CLUSTER_TIME, null, null);
    }
}
