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

package com.hazelcast.ringbuffer.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RingbufferMigrationTest extends HazelcastTestSupport {

    public static final int CAPACITY = 100;
    public static final String BOUNCING_TEST_PARTITION_COUNT = "10";
    private TestHazelcastInstanceFactory instanceFactory;

    @Before
    public void setup() {
        instanceFactory = createHazelcastInstanceFactory(3);
    }

    @Test
    public void test() throws Exception {
        final String ringbufferName = "ringbuffer";
        final Config config = new Config()
                .addRingBufferConfig(new RingbufferConfig(ringbufferName).setTimeToLiveSeconds(0));
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), BOUNCING_TEST_PARTITION_COUNT);
        HazelcastInstance hz1 = instanceFactory.newHazelcastInstance(config);

        for (int k = 0; k < 10 * CAPACITY; k++) {
            hz1.getRingbuffer(ringbufferName).add(k);
        }

        long oldTailSeq = hz1.getRingbuffer(ringbufferName).tailSequence();
        long oldHeadSeq = hz1.getRingbuffer(ringbufferName).headSequence();

        HazelcastInstance hz2 = instanceFactory.newHazelcastInstance(config);
        HazelcastInstance hz3 = instanceFactory.newHazelcastInstance(config);

        assertClusterSizeEventually(3, hz2);
        waitAllForSafeState(hz1, hz2, hz3);
        hz1.shutdown();
        assertClusterSizeEventually(2, hz2);
        waitAllForSafeState(hz2, hz3);

        assertEquals(oldTailSeq, hz2.getRingbuffer(ringbufferName).tailSequence());
        assertEquals(oldHeadSeq, hz2.getRingbuffer(ringbufferName).headSequence());
    }
}
