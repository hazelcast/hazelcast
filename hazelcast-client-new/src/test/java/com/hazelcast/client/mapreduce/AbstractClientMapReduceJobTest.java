/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.mapreduce;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastTestSupport;

public abstract class AbstractClientMapReduceJobTest extends HazelcastTestSupport {

    protected Config buildConfig() {
        Config config = new XmlConfigBuilder().build();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        return config;
    }

    protected void prepareClusterNodes(int numberOfNodes) {
        Config config = buildConfig();
        HazelcastInstance[] instances = new HazelcastInstance[numberOfNodes];
        for (int i = 0; i < numberOfNodes; i++) {
            instances[i] = Hazelcast.newHazelcastInstance(config);
        }

        for (int i = 0; i < numberOfNodes; i++) {
            assertClusterSizeEventually(numberOfNodes, instances[i]);
        }
    }

    protected IMap<Integer, Integer> getFilledIMap(HazelcastInstance client, int numberOfEntries) {
        IMap<Integer, Integer> m1 = client.getMap(randomMapName());
        for (int i = 0; i < numberOfEntries; i++) {
            m1.put(i, i);
        }
        return m1;
    }


}
