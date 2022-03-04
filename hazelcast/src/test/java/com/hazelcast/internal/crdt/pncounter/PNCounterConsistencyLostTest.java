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

package com.hazelcast.internal.crdt.pncounter;

import com.hazelcast.config.CRDTReplicationConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.ConsistencyLostException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.cluster.Address;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

/**
 * Member implementation for testing behaviour of {@link ConsistencyLostException}
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PNCounterConsistencyLostTest extends AbstractPNCounterConsistencyLostTest {

    private HazelcastInstance[] members;
    private HazelcastInstance liteMember;

    @Before
    public void setup() {
        final Config dataConfig = new Config()
                .setProperty(ClusterProperty.PARTITION_COUNT.getName(), "5")
                .setCRDTReplicationConfig(new CRDTReplicationConfig()
                        .setReplicationPeriodMillis(Integer.MAX_VALUE)
                        .setMaxConcurrentReplicationTargets(Integer.MAX_VALUE));
        final Config liteConfig = new Config()
                .setProperty(ClusterProperty.PARTITION_COUNT.getName(), "5")
                .setLiteMember(true);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        members = factory.newInstances(dataConfig, 2);
        liteMember = factory.newHazelcastInstance(liteConfig);
    }


    @Override
    protected HazelcastInstance[] getMembers() {
        return members;
    }

    @Override
    protected Address getCurrentTargetReplicaAddress(PNCounter driver) {
        return ((PNCounterProxy) driver).getCurrentTargetReplicaAddress();
    }

    @Override
    protected void assertState(PNCounter driver) {
        assertEquals(5, driver.get());
    }

    @Override
    protected void mutate(PNCounter driver) {
        driver.addAndGet(5);
    }

    @Override
    protected PNCounter getCounter() {
        final PNCounter counter = liteMember.getPNCounter("counter");
        ((PNCounterProxy) counter).setOperationTryCount(1);
        return counter;
    }
}
