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

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Test behaviour of PN counter when there are no data members in the cluster
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PNCounterNoDataMemberTest extends AbstractPNCounterNoDataMemberTest {

    private HazelcastInstance liteMember;
    private String counterName = randomMapName("counter-");

    @Before
    public void setup() {
        final Config liteConfig = new Config()
                .setProperty(ClusterProperty.PARTITION_COUNT.getName(), "5")
                .setLiteMember(true);

        liteMember = createHazelcastInstanceFactory().newHazelcastInstance(liteConfig);
    }

    @Override
    protected void mutate(PNCounter driver) {
        driver.addAndGet(100);
    }

    @Override
    protected PNCounter getCounter() {
        final PNCounter counter = liteMember.getPNCounter(counterName);
        ((PNCounterProxy) counter).setOperationTryCount(1);
        return counter;
    }
}
