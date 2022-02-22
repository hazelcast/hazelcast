/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class AutoDetectionJoinTest extends AbstractJoinTest {

    @Before
    @After
    public void killAllHazelcastInstances() {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    @Category(QuickTest.class)
    public void defaultConfig() throws Exception {
        testJoinEventually(new Config());
    }

    @Test
    public void interfacesEnabled() throws Exception {
        Config config = new Config();
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface(pickLocalInetAddress().getHostAddress());
        testJoinEventually(config);
    }

    @Test
    public void differentClusterNames() throws Exception {
        Config config1 = new Config();
        config1.setClusterName("cluster1");

        Config config2 = new Config();
        config2.setClusterName("cluster2");

        assertIndependentClusters(config1, config2);
    }

    @Test
    public void autoDetectionDisabled() throws Exception {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);

        assertIndependentClusters(config, config);
    }

    @Test
    public void notUsedWhenOtherDiscoveryEnabled() throws Exception {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);

        assertIndependentClusters(config, config);
    }


}
