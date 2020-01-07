/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.partition.impl.InternalPartitionImpl;
import com.hazelcast.cluster.Address;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.RandomPicker;
import com.hazelcast.internal.util.UuidUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static com.hazelcast.internal.partition.InternalPartition.MAX_REPLICA_COUNT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PartitionTableViewTest {

    @Test
    public void test_getVersion() {
        int version = RandomPicker.getInt(1000);
        PartitionTableView table = new PartitionTableView(new PartitionReplica[10][MAX_REPLICA_COUNT], version);
        assertEquals(version, table.getVersion());
    }

    @Test
    public void test_getLength() {
        int len = RandomPicker.getInt(100);
        PartitionTableView table = new PartitionTableView(new PartitionReplica[len][MAX_REPLICA_COUNT], 0);
        assertEquals(len, table.getLength());
    }

    @Test
    public void test_getMember() throws Exception {
        PartitionReplica[][] members = createRandomMembers();
        PartitionTableView table = new PartitionTableView(members, 0);

        assertEquals(members.length, table.getLength());
        for (int i = 0; i < members.length; i++) {
            for (int j = 0; j < MAX_REPLICA_COUNT; j++) {
                assertEquals(members[i][j], table.getReplica(i, j));
            }
        }
    }

    @Test
    public void test_getMembers() throws Exception {
        PartitionReplica[][] members = createRandomMembers();
        PartitionTableView table = new PartitionTableView(members, 0);

        assertEquals(members.length, table.getLength());
        for (int i = 0; i < members.length; i++) {
            PartitionReplica[] replicas = table.getReplicas(i);
            assertNotSame(members[i], replicas);
            assertArrayEquals(members[i], replicas);
        }
    }

    @Test
    public void test_getMembers_withNullAddress() {
        PartitionReplica[][] members = new PartitionReplica[100][MAX_REPLICA_COUNT];
        PartitionTableView table = new PartitionTableView(members, 0);

        assertEquals(members.length, table.getLength());
        for (int i = 0; i < members.length; i++) {
            PartitionReplica[] replicas = table.getReplicas(i);
            assertNotSame(members[i], replicas);
            assertArrayEquals(members[i], replicas);
        }
    }

    @Test
    public void test_createUsingInternalPartitions() throws Exception {
        PartitionReplica[][] members = createRandomMembers();
        InternalPartition[] partitions = new InternalPartition[members.length];
        for (int i = 0; i < partitions.length; i++) {
            partitions[i] = new InternalPartitionImpl(i, null, members[i][0], members[i]);
        }

        PartitionTableView table = new PartitionTableView(partitions, 0);
        assertEquals(partitions.length, table.getLength());

        for (int i = 0; i < members.length; i++) {
            for (int j = 0; j < InternalPartition.MAX_REPLICA_COUNT; j++) {
                assertEquals(partitions[i].getReplica(j), table.getReplica(i, j));
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
        PartitionTableView table2 = new PartitionTableView(extractPartitionTableMembers(table1), table1.getVersion());

        assertEquals(table1, table2);
        assertEquals(table1.hashCode(), table2.hashCode());
    }

    @Test
    public void testEquals_whenVersionIsDifferent() throws Exception {
        PartitionTableView table1 = createRandomPartitionTable();
        PartitionTableView table2 = new PartitionTableView(extractPartitionTableMembers(table1), table1.getVersion() + 1);

        assertNotEquals(table1, table2);
    }

    @Test
    public void testEquals_whenSingleAddressIsDifferent() throws Exception {
        PartitionTableView table1 = createRandomPartitionTable();
        PartitionReplica[][] addresses = extractPartitionTableMembers(table1);
        PartitionReplica member = addresses[addresses.length - 1][MAX_REPLICA_COUNT - 1];
        Address newAddress = new Address(member.address().getInetAddress(), member.address().getPort() + 1);
        addresses[addresses.length - 1][MAX_REPLICA_COUNT - 1] = new PartitionReplica(newAddress, UuidUtil.newUnsecureUUID());
        PartitionTableView table2 = new PartitionTableView(addresses, table1.getVersion());

        assertNotEquals(table1, table2);
    }

    private static PartitionTableView createRandomPartitionTable() throws UnknownHostException {
        PartitionReplica[][] members = createRandomMembers();
        return new PartitionTableView(members, RandomPicker.getInt(1000));
    }

    private static PartitionReplica[][] createRandomMembers() throws UnknownHostException {
        InetAddress localAddress = InetAddress.getLocalHost();
        PartitionReplica[][] addresses = new PartitionReplica[100][MAX_REPLICA_COUNT];
        for (int i = 0; i < addresses.length; i++) {
            for (int j = 0; j < MAX_REPLICA_COUNT; j++) {
                Address address = new Address("10.10." + i + "." + RandomPicker.getInt(256), localAddress, 5000 + j);
                addresses[i][j] = new PartitionReplica(address, UuidUtil.newUnsecureUUID());
            }
        }
        return addresses;
    }

    private static PartitionReplica[][] extractPartitionTableMembers(PartitionTableView table) {
        PartitionReplica[][] members = new PartitionReplica[table.getLength()][];
        for (int i = 0; i < members.length; i++) {
            members[i] = table.getReplicas(i);
        }
        return members;
    }
}
