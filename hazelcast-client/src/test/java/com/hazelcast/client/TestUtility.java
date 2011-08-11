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

import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class TestUtility {
    
    static final List<HazelcastClient> clients = new CopyOnWriteArrayList<HazelcastClient>(); 

    public synchronized static void destroyClients(){
        for(final HazelcastClient c : clients){
            c.shutdown();
        }
        clients.clear();
    }
    
    public synchronized static HazelcastClient newHazelcastClient(HazelcastInstance... h) {
        String name = h[0].getConfig().getGroupConfig().getName();
        String pass = h[0].getConfig().getGroupConfig().getPassword();
        return newHazelcastClient(ClientProperties.createBaseClientProperties(name, pass), h);
    }
    
    public synchronized static HazelcastClient newHazelcastClient(ClientProperties properties, HazelcastInstance... h) {
        InetSocketAddress[] addresses = new InetSocketAddress[h.length];
        for (int i = 0; i < h.length; i++) {
            addresses[i] = h[i].getCluster().getLocalMember().getInetSocketAddress();
        }
        final HazelcastClient client = HazelcastClient.newHazelcastClient(properties, true, addresses);
        clients.add(client);
        return client;
    }

	public synchronized static HazelcastClient getAutoUpdatingClient(HazelcastInstance h1) {
		String address = h1.getCluster().getLocalMember().getInetSocketAddress().toString().substring(1);
		GroupConfig gc = h1.getConfig().getGroupConfig();
		HazelcastClient client = HazelcastClient.newHazelcastClient(gc.getName(), gc.getPassword(), address);
		clients.add(client);
		return client;
	}
}
