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
package com.hazelcast.instance;


import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.*;
import com.hazelcast.test.annotation.*;
import org.junit.*;
import org.junit.experimental.categories.*;
import org.junit.runner.*;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class DefaultAddressPickerTest extends HazelcastTestSupport {

    @Test
    public void port_picked_by_OS_when_set_zero() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance(createMulticastConfig(0));
        int port = hazelcastInstance.getCluster().getLocalMember().getSocketAddress().getPort();
        assertThat("Port must be auto-picked by OS when configured as 0", port, not(0));
    }

    @Test
    public void cluster_formed_when_port_picked_by_OS() {
        int noOfMembers = 2;
        Config config = createMulticastConfig(0);
        HazelcastInstance[] instances = createHazelcastInstanceFactory(noOfMembers).newInstances(config);
        assertThat(instances[0].getCluster().getMembers().size(), is(noOfMembers));
    }

    private Config createMulticastConfig(int port) {
        Config config = new Config();
        config.getNetworkConfig().setPort(port);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(true);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getAwsConfig().setEnabled(false);
        return config;
    }
}
