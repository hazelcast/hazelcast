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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class SplitMergeTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

    @Test
    public void test_memberUuidChanges_duringMerge() {
        HazelcastInstance h1 = factory.newHazelcastInstance(newConfig());
        HazelcastInstance h2 = factory.newHazelcastInstance(newConfig());

        String initialUuid_H1 = getNode(h1).getThisUuid();
        String initialUuid_H2 = getNode(h2).getThisUuid();

        // create split
        closeConnectionBetween(h1, h2);
        assertClusterSizeEventually(1, h1);
        assertClusterSizeEventually(1, h2);

        // merge back
        mergeBack(h2, getAddress(h1));
        assertClusterSizeEventually(2, h1);
        assertClusterSizeEventually(2, h2);

        String currentUuid_H1 = getNode(h1).getThisUuid();
        String currentUuid_H2 = getNode(h2).getThisUuid();

        // h2 merges to h1.
        // UUID of h1 remains the same.
        // UUID of h2 should change.
        assertEquals(initialUuid_H1, currentUuid_H1);
        assertNotEquals(initialUuid_H2, currentUuid_H2);

        assertNotNull(getClusterService(h1).getMember(currentUuid_H1));
        assertNotNull(getClusterService(h1).getMember(currentUuid_H2));

        assertNotNull(getClusterService(h2).getMember(currentUuid_H1));
        assertNotNull(getClusterService(h2).getMember(currentUuid_H2));
    }

    private void mergeBack(HazelcastInstance hz, Address to) {
        getNode(hz).getClusterService().merge(to);
    }

    private Config newConfig() {
        Config config = new Config();
        // to avoid accidental merge
        config.setProperty(GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "600");
        config.setProperty(GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "600");
        return config;
    }
}
