/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.management;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.management.dto.PartitionServiceBeanDTO;
import com.hazelcast.internal.monitor.impl.MemberStateImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PartitionServiceBeanDTOTest extends HazelcastTestSupport {

    private HazelcastInstance instance;

    @After
    public void shutdown() {
        if (instance != null) {
            instance.shutdown();
        }
    }

    /**
     * https://github.com/hazelcast/hazelcast/issues/8463
     */
    @Test
    public void testJMXStatsWithPublicAddressHostName() {
        String publicAddress = "hazelcast.org";
        instance = createInstanceWithPublicAddress(publicAddress);
        assumeNotNull(instance, "Internet access and/or DNS resolver are not available, "
                + publicAddress + " cannot be resolved!");

        warmUpPartitions(instance);

        MemberStateImpl memberState = new MemberStateImpl();
        TimedMemberStateFactoryHelper.registerJMXBeans(getNode(instance).hazelcastInstance, memberState);
        PartitionServiceBeanDTO partitionServiceDTO = memberState.getMXBeans().getPartitionServiceBean();
        assertEquals(partitionServiceDTO.getPartitionCount(), partitionServiceDTO.getActivePartitionCount());
    }

    private HazelcastInstance createInstanceWithPublicAddress(String publicAddress) {
        try {
            Config config = getConfig();
            NetworkConfig networkConfig = config.getNetworkConfig();
            networkConfig.setPublicAddress(publicAddress);
            return Hazelcast.newHazelcastInstance(config);
        } catch (RuntimeException e) {
            if (e.getCause() instanceof UnknownHostException) {
                // internet access and/or DNS resolver are not available
                return null;
            }
            throw e;
        }
    }

    @Override
    protected Config getConfig() {
        Config config = new Config();

        // Join is disabled intentionally. will start standalone HazelcastInstance.
        NetworkConfig networkConfig = config.getNetworkConfig();
        JoinConfig join = networkConfig.getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);

        return config;
    }
}
