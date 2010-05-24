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

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.impl.GroupProperties;

import java.net.InetSocketAddress;

public class TestUtility {
    public static HazelcastClient client;
    static HazelcastInstance hz;

    public synchronized static HazelcastClient getHazelcastClient() {
        if (client == null) {
            Config config = new Config();
            config.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "1");
            hz = Hazelcast.newHazelcastInstance(config);
            client = getHazelcastClient(hz);
        }
        return client;
    }

    public synchronized static void shutdownHazelcastClient() {
        if (client != null) {
            client.shutdown();
        }
    }

    public synchronized static HazelcastClient getHazelcastClient(HazelcastInstance... h) {
        InetSocketAddress[] addresses = new InetSocketAddress[h.length];
        for (int i = 0; i < h.length; i++) {
            addresses[i] = h[i].getCluster().getLocalMember().getInetSocketAddress();
        }
        String name = h[0].getConfig().getGroupConfig().getName();
        String pass = h[0].getConfig().getGroupConfig().getPassword();
        HazelcastClient client = HazelcastClient.newHazelcastClient(name, pass, true, addresses);
        return client;
    }
}
