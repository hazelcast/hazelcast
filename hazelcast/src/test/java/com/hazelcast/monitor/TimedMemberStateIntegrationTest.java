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

package com.hazelcast.monitor;

import com.hazelcast.config.Config;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.management.TimedMemberStateFactory;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Set;

import static com.hazelcast.instance.TestUtil.getHazelcastInstanceImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class TimedMemberStateIntegrationTest extends HazelcastTestSupport {

    @Test
    public void testServices() {
        HazelcastInstance hz = createHazelcastInstance();
        TimedMemberStateFactory factory = new TimedMemberStateFactory(getHazelcastInstanceImpl(hz));

        hz.getMap("trial").put(1, 1);
        hz.getMultiMap("trial").put(2, 2);
        hz.getQueue("trial").offer(3);
        hz.getTopic("trial").publish("Hello");
        hz.getReplicatedMap("trial").put(3, 3);
        hz.getExecutorService("trial");

        TimedMemberState timedMemberState = factory.createTimedMemberState();
        Set<String> instanceNames = timedMemberState.getInstanceNames();

        assertEquals("dev", timedMemberState.clusterName);
        assertContains(instanceNames, "c:trial");
        assertContains(instanceNames, "m:trial");
        assertContains(instanceNames, "q:trial");
        assertContains(instanceNames, "t:trial");
        assertContains(instanceNames, "r:trial");
        assertContains(instanceNames, "e:trial");
    }

    @Test
    public void testSSL_defaultConfig() {
        HazelcastInstance hz = createHazelcastInstance();
        TimedMemberStateFactory factory = new TimedMemberStateFactory(getHazelcastInstanceImpl(hz));

        TimedMemberState timedMemberState = factory.createTimedMemberState();
        assertFalse(timedMemberState.sslEnabled);
    }

    @Test
    public void testSSL_enabled() {
        testSSL(true);
    }

    @Test
    public void testSSL_disabled() {
        testSSL(false);
    }

    private void testSSL(boolean enabled) {
        SSLConfig sslConfig = new SSLConfig();
        sslConfig.setEnabled(enabled);

        Config config = getConfig();
        config.getNetworkConfig().setSSLConfig(sslConfig);

        HazelcastInstance hz = createHazelcastInstance(config);
        TimedMemberStateFactory factory = new TimedMemberStateFactory(getHazelcastInstanceImpl(hz));

        TimedMemberState timedMemberState = factory.createTimedMemberState();
        assertEquals(enabled, timedMemberState.sslEnabled);
    }
}
