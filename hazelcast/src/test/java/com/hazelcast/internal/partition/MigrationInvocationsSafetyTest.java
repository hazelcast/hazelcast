/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.hazelcast.instance.TestUtil.terminateInstance;
import static com.hazelcast.internal.partition.impl.PartitionDataSerializerHook.FETCH_PARTITION_STATE;
import static com.hazelcast.internal.partition.impl.PartitionDataSerializerHook.F_ID;
import static com.hazelcast.internal.partition.impl.PartitionDataSerializerHook.PARTITION_STATE_OP;
import static com.hazelcast.test.PacketFiltersUtil.dropOperationsBetween;
import static com.hazelcast.test.PacketFiltersUtil.resetPacketFiltersFrom;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MigrationInvocationsSafetyTest extends PartitionCorrectnessTestSupport {

    @Before
    public void setupParams() {
        nodeCount = 4;
        backupCount = nodeCount - 1;
    }

    @Test
    public void members_shouldAgree_onPartitionTable_whenMasterChanges() {
        final HazelcastInstance initialMaster = factory.newHazelcastInstance();
        final HazelcastInstance nextMaster = factory.newHazelcastInstance();
        final HazelcastInstance slave1 = factory.newHazelcastInstance();
        final HazelcastInstance slave2 = factory.newHazelcastInstance();
        final HazelcastInstance slave3 = factory.newHazelcastInstance();

        assertClusterSizeEventually(5, nextMaster, slave1, slave2, slave3);

        // nextMaster & slave1 won't receive partition table updates from initialMaster.
        dropOperationsBetween(initialMaster, asList(slave1, nextMaster), F_ID, singletonList(PARTITION_STATE_OP));

        warmUpPartitions(initialMaster, slave2, slave3);

        final int initialPartitionStateVersion = getPartitionService(initialMaster).getPartitionStateVersion();
        assertEquals(initialPartitionStateVersion, getPartitionService(slave2).getPartitionStateVersion());
        assertEquals(initialPartitionStateVersion, getPartitionService(slave3).getPartitionStateVersion());
        assertEquals(0, getPartitionService(nextMaster).getPartitionStateVersion());
        assertEquals(0, getPartitionService(slave1).getPartitionStateVersion());

        dropOperationsBetween(nextMaster, slave3, F_ID, singletonList(FETCH_PARTITION_STATE));

        terminateInstance(initialMaster);

        spawn(new Runnable() {
            @Override
            public void run() {
                assertClusterSizeEventually(4, nextMaster, slave1, slave2, slave3);
                sleepSeconds(10);
                resetPacketFiltersFrom(nextMaster);
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                int nextPartitionStateVersion = getPartitionService(nextMaster).getPartitionStateVersion();
                assertThat(nextPartitionStateVersion, greaterThan(initialPartitionStateVersion));

                assertEquals(nextPartitionStateVersion, getPartitionService(slave1).getPartitionStateVersion());
                assertEquals(nextPartitionStateVersion, getPartitionService(slave2).getPartitionStateVersion());
                assertEquals(nextPartitionStateVersion, getPartitionService(slave3).getPartitionStateVersion());
            }
        });

    }

    @Test
    public void members_shouldAgree_onPartitionTable_whenMasterChanges_and_anotherMemberCrashes() {
        final HazelcastInstance initialMaster = factory.newHazelcastInstance();
        final HazelcastInstance nextMaster = factory.newHazelcastInstance();
        final HazelcastInstance slave1 = factory.newHazelcastInstance();
        final HazelcastInstance slave2 = factory.newHazelcastInstance();
        final HazelcastInstance slave3 = factory.newHazelcastInstance();

        assertClusterSizeEventually(5, nextMaster, slave1, slave2, slave3);

        // nextMaster & slave1 won't receive partition table updates from initialMaster.
        dropOperationsBetween(initialMaster, asList(slave1, nextMaster), F_ID, singletonList(PARTITION_STATE_OP));

        warmUpPartitions(initialMaster, slave2, slave3);

        final int initialPartitionStateVersion = getPartitionService(initialMaster).getPartitionStateVersion();
        assertEquals(initialPartitionStateVersion, getPartitionService(slave2).getPartitionStateVersion());
        assertEquals(initialPartitionStateVersion, getPartitionService(slave3).getPartitionStateVersion());
        assertEquals(0, getPartitionService(nextMaster).getPartitionStateVersion());
        assertEquals(0, getPartitionService(slave1).getPartitionStateVersion());

        dropOperationsBetween(nextMaster, slave3, F_ID, singletonList(FETCH_PARTITION_STATE));

        terminateInstance(initialMaster);

        spawn(new Runnable() {
            @Override
            public void run() {
                assertClusterSizeEventually(4, nextMaster, slave1, slave2, slave3);
                sleepSeconds(10);
                terminateInstance(slave3);
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                int nextPartitionStateVersion = getPartitionService(nextMaster).getPartitionStateVersion();
                assertThat(nextPartitionStateVersion, greaterThan(initialPartitionStateVersion));

                assertEquals(nextPartitionStateVersion, getPartitionService(slave1).getPartitionStateVersion());
                assertEquals(nextPartitionStateVersion, getPartitionService(slave2).getPartitionStateVersion());
            }
        });

    }
}
