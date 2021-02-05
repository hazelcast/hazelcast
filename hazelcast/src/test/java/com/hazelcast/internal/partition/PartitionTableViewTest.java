/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.util.RandomPicker;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static com.hazelcast.internal.partition.InternalPartition.MAX_REPLICA_COUNT;
import static com.hazelcast.internal.partition.PartitionStamp.calculateStamp;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PartitionTableViewTest {

    @Test
    public void test_getStamp() throws Exception {
        InternalPartition[] partitions = createRandomPartitions();
        PartitionTableView table = new PartitionTableView(partitions);
        assertEquals(calculateStamp(partitions), table.stamp());
    }

    @Test
    public void test_getLength() {
        int len = RandomPicker.getInt(100);
        PartitionTableView table = new PartitionTableView(new InternalPartition[len]);
        assertEquals(len, table.length());
    }

    @Test
    public void test_getReplica() throws Exception {
        InternalPartition[] partitions = createRandomPartitions();
        PartitionTableView table = new PartitionTableView(partitions);

        assertEquals(partitions.length, table.length());

        for (int i = 0; i < partitions.length; i++) {
            for (int j = 0; j < MAX_REPLICA_COUNT; j++) {
                assertEquals(partitions[i].getReplica(j), table.getReplica(i, j));
            }
        }
    }

    @Test
    public void test_getReplicas() throws Exception {
        InternalPartition[] partitions = createRandomPartitions();
        PartitionTableView table = new PartitionTableView(partitions);

        assertEquals(partitions.length, table.length());
        for (int i = 0; i < partitions.length; i++) {
            PartitionReplica[] replicas = table.getReplicas(i);
            assertNotSame(partitions[i], replicas);
            for (int j = 0; j < MAX_REPLICA_COUNT; j++) {
                assertEquals(partitions[i].getReplica(j), replicas[j]);
            }
        }
    }

    @Test
    public void testIdentical() throws Exception {
        PartitionTableView table = createRandomPartitionTable();
        assertEquals(table, table);
    }

    @Test
    public void testEquals() throws Exception {
        PartitionTableView table1 = createRandomPartitionTable();
        PartitionTableView table2 = new PartitionTableView(extractPartitions(table1));

        assertEquals(table1, table2);
        assertEquals(table1.hashCode(), table2.hashCode());
    }

    @Test
    public void testEquals_whenSingleReplicaIsDifferent() throws Exception {
        PartitionTableView table1 = createRandomPartitionTable();

        InternalPartition[] partitions = extractPartitions(table1);
        PartitionReplica[] replicas = table1.getReplicas(0);
        PartitionReplica replica = replicas[0];
        Address newAddress = new Address(replica.address().getInetAddress(), replica.address().getPort() + 1);
        replicas[0] = new PartitionReplica(newAddress, UuidUtil.newUnsecureUUID());
        partitions[0] = new ReadonlyInternalPartition(replicas, 0, partitions[0].version());

        PartitionTableView table2 = new PartitionTableView(partitions);

        assertNotEquals(table1, table2);
    }

    private static PartitionTableView createRandomPartitionTable() throws UnknownHostException {
        return new PartitionTableView(createRandomPartitions());
    }

    private static InternalPartition[] createRandomPartitions() throws UnknownHostException {
        InetAddress localAddress = InetAddress.getLocalHost();
        InternalPartition[] partitions = new InternalPartition[100];
        for (int i = 0; i < partitions.length; i++) {
            PartitionReplica[] replicas = new PartitionReplica[MAX_REPLICA_COUNT];
            for (int j = 0; j < MAX_REPLICA_COUNT; j++) {
                Address address = new Address("10.10." + i + "." + RandomPicker.getInt(256), localAddress, 5000 + j);
                replicas[j] = new PartitionReplica(address, UuidUtil.newUnsecureUUID());
            }
            partitions[i] = new ReadonlyInternalPartition(replicas, i, RandomPicker.getInt(1, 10));
        }
        return partitions;
    }

    private static InternalPartition[] extractPartitions(PartitionTableView table) {
        InternalPartition[] partitions = new InternalPartition[table.length()];
        for (int i = 0; i < partitions.length; i++) {
            partitions[i] = table.getPartition(i);
        }
        return partitions;
    }
}
