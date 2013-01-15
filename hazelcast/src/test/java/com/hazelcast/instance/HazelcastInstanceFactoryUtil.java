/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.instance;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.impl.StaticNodeRegistry;

import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicInteger;

public class HazelcastInstanceFactoryUtil {

    final static AtomicInteger ports = new AtomicInteger(5000);

    public static HazelcastInstance[] newInstances(int count) {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        Address[] addresses = new Address[count];
        for (int i = 0; i < count; i++) {
            try {
                addresses[i] = new Address("127.0.0.1", ports.incrementAndGet());
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }
        HazelcastInstance[] instances = new HazelcastInstance[count];
        StaticNodeRegistry staticNodeRegistry = new StaticNodeRegistry(addresses);
        for (int i = 0; i < count; i++) {
            NodeContext nodeContext = staticNodeRegistry.createNodeContext(addresses[i]);
            instances[i] = HazelcastInstanceFactory.newHazelcastInstance(config, null, nodeContext);
        }
        return instances;
    }
}
