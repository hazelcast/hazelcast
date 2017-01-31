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

package com.hazelcast.internal.partition;

import com.hazelcast.internal.partition.impl.InternalPartitionImpl;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.RandomPicker;
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
@Category({QuickTest.class, ParallelTest.class})
public class PartitionTableViewTest {

    @Test
    public void test_getVersion() throws Exception {
        int version = RandomPicker.getInt(1000);
        PartitionTableView table = new PartitionTableView(new Address[10][MAX_REPLICA_COUNT], version);
        assertEquals(version, table.getVersion());
    }

    @Test
    public void test_getLength() throws Exception {
        int len = RandomPicker.getInt(100);
        PartitionTableView table = new PartitionTableView(new Address[len][MAX_REPLICA_COUNT], 0);
        assertEquals(len, table.getLength());
    }

    @Test
    public void test_getAddress() throws Exception {
        Address[][] addresses = createRandomAddresses();
        PartitionTableView table = new PartitionTableView(addresses, 0);

        assertEquals(addresses.length, table.getLength());
        for (int i = 0; i < addresses.length; i++) {
            for (int j = 0; j < MAX_REPLICA_COUNT; j++) {
                assertEquals(addresses[i][j], table.getAddress(i, j));
            }
        }
    }

    @Test
    public void test_getAddresses() throws Exception {
        Address[][] addresses = createRandomAddresses();
        PartitionTableView table = new PartitionTableView(addresses, 0);

        assertEquals(addresses.length, table.getLength());
        for (int i = 0; i < addresses.length; i++) {
            Address[] replicas = table.getAddresses(i);
            assertNotSame(addresses[i], replicas);
            assertArrayEquals(addresses[i], replicas);
        }
    }

    @Test
    public void test_getAddresses_withNullAddress() throws Exception {
        Address[][] addresses = new Address[100][MAX_REPLICA_COUNT];
        PartitionTableView table = new PartitionTableView(addresses, 0);

        assertEquals(addresses.length, table.getLength());
        for (int i = 0; i < addresses.length; i++) {
            Address[] replicas = table.getAddresses(i);
            assertNotSame(addresses[i], replicas);
            assertArrayEquals(addresses[i], replicas);
        }
    }

    @Test
    public void test_createUsingInternalPartitions() throws Exception {
        Address[][] addresses = createRandomAddresses();
        InternalPartition[] partitions = new InternalPartition[addresses.length];
        for (int i = 0; i < partitions.length; i++) {
            partitions[i] = new InternalPartitionImpl(i, null, null, addresses[i]);
        }

        PartitionTableView table = new PartitionTableView(partitions, 0);
        assertEquals(partitions.length, table.getLength());

        for (int i = 0; i < addresses.length; i++) {
            for (int j = 0; j < InternalPartition.MAX_REPLICA_COUNT; j++) {
                assertEquals(partitions[i].getReplicaAddress(j), table.getAddress(i, j));
            }
        }
    }

    @Test
    public void test_writeAndReadData() throws Exception {
        InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();

        PartitionTableView table1 = createRandomPartitionTable();
        BufferObjectDataOutput out = serializationService.createObjectDataOutput();
        PartitionTableView.writeData(table1, out);

        BufferObjectDataInput in = serializationService.createObjectDataInput(out.toByteArray());
        PartitionTableView table2 = PartitionTableView.readData(in);

        assertEquals(table1, table2);
        assertEquals(table1.hashCode(), table2.hashCode());
    }

    @Test
    public void testIdentical() throws Exception {
        PartitionTableView table = createRandomPartitionTable();
        assertEquals(table, table);
    }

    @Test
    public void testEquals() throws Exception {
        PartitionTableView table1 = createRandomPartitionTable();
        PartitionTableView table2 = new PartitionTableView(extractPartitionTableAddresses(table1), table1.getVersion());

        assertEquals(table1, table2);
        assertEquals(table1.hashCode(), table2.hashCode());
    }

    @Test
    public void testEquals_whenVersionIsDifferent() throws Exception {
        PartitionTableView table1 = createRandomPartitionTable();
        PartitionTableView table2 = new PartitionTableView(extractPartitionTableAddresses(table1), table1.getVersion() + 1);

        assertNotEquals(table1, table2);
    }

    @Test
    public void testEquals_whenSingleAddressIsDifferent() throws Exception {
        PartitionTableView table1 = createRandomPartitionTable();
        Address[][] addresses = extractPartitionTableAddresses(table1);
        Address address = addresses[addresses.length - 1][MAX_REPLICA_COUNT - 1];
        addresses[addresses.length - 1][MAX_REPLICA_COUNT - 1] = new Address(address.getInetAddress(), address.getPort() + 1);
        PartitionTableView table2 = new PartitionTableView(addresses, table1.getVersion());

        assertNotEquals(table1, table2);
    }

    private static PartitionTableView createRandomPartitionTable() throws UnknownHostException {
        Address[][] addresses = createRandomAddresses();
        return new PartitionTableView(addresses, RandomPicker.getInt(1000));
    }

    private static Address[][] createRandomAddresses() throws UnknownHostException {
        InetAddress localAddress = InetAddress.getLocalHost();
        Address[][] addresses = new Address[100][MAX_REPLICA_COUNT];
        for (int i = 0; i < addresses.length; i++) {
            for (int j = 0; j < MAX_REPLICA_COUNT; j++) {
                addresses[i][j] = new Address("10.10." + i + "." + RandomPicker.getInt(256), localAddress, 5000 + j);
            }
        }
        return addresses;
    }

    private static Address[][] extractPartitionTableAddresses(PartitionTableView table) {
        Address[][] addresses = new Address[table.getLength()][];
        for (int i = 0; i < addresses.length; i++) {
            addresses[i] = table.getAddresses(i);
        }
        return addresses;
    }
}
