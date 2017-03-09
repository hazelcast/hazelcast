/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;


@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class SlowMulticastJoinTest extends AbstractJoinTest {

    @Before
    @After
    public void killAllHazelcastInstances() throws IOException {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void testMembersStaysIndependentWhenHostIsNotTrusted() {
        Config config1 = newConfig("8.8.8.8"); //8.8.8.8 is never a local address
        Config config2 = newConfig("8.8.8.8");

        int testDurationSeconds = 30;
        assertIndependentClustersAndDoNotMergedEventually(config1, config2, testDurationSeconds);
    }

    @Test
    public void testMembersFormAClusterWhenHostIsTrusted() throws Exception {
        Config config2 = newConfig("*.*.*.*"); //matching everything

        testJoin(config2);
    }

    private Config newConfig(String trustedInterface) {
        Config config = new Config();
        config.setProperty(GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "5");
        config.setProperty(GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "3");
        config.getNetworkConfig().getJoin().getMulticastConfig().addTrustedInterface(trustedInterface);
        return config;
    }
}
