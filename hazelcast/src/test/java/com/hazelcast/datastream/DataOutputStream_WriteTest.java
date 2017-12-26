/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.datastream;

import com.hazelcast.config.Config;
import com.hazelcast.config.DataStreamConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Ignore;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static com.hazelcast.spi.properties.GroupProperty.PARTITION_COUNT;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class DataOutputStream_WriteTest extends HazelcastTestSupport {

    @Test
    public void whenVariableLengthAndFewItems() throws InterruptedException {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "1")
                .addDataStreamConfig(new DataStreamConfig("employees")
                        .setInitialRegionSize(1024));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataStream<String> stream = hz.getDataStream("employees");
        DataOutputStream<String> out = stream.newOutputStream();
        out.write("foo", "1");
        System.out.println(stream.tail("foo"));
        out.write("foo", "22");
        System.out.println(stream.tail("foo"));
        out.write("foo", "333");
        System.out.println(stream.tail("foo"));

        assertEquals(3, stream.asFrame().count());
        assertEquals(1, stream.asFrame().memoryInfo().regionsInUse());
        assertEquals(54, stream.tail("foo"));
        assertEquals(0, stream.head("foo"));

        DataInputStream in = stream.newInputStream(0, 0);
        assertEquals("1", in.read());
        assertEquals("22", in.read());
        assertEquals("333", in.read());
        assertNull(in.poll());
    }

    @Ignore
    @Test
    public void whenVariableLengthAndManyItems() throws InterruptedException {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "1")
                .addDataStreamConfig(new DataStreamConfig("employees")
                        .setInitialRegionSize(128)
                        .setMaxRegionSize(128));

        HazelcastInstance hz = createHazelcastInstance(config);
        List<Employee> produced = new LinkedList<>();
        DataStream<Employee> stream = hz.getDataStream("employees");
        DataOutputStream<Employee> out = stream.newOutputStream();
        int count = 5;
        for (int k = 0; k < count; k++) {
            Employee record = new Employee(k, k, k);
            produced.add(record);
            out.write(record);
        }

        assertEquals(count, stream.asFrame().count());

        DataInputStream<Employee> in = stream.newInputStream(0, 0);
        List<Employee> consumed = new LinkedList<>();
        for (int k = 0; k < count; k++) {
            Employee read = in.read();
            System.out.println(read);
            consumed.add(read);
        }

        assertNull(in.poll());
        assertEquals(produced, consumed);
    }

    @Test
    public void whenSimple() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "1")
                .addDataStreamConfig(new DataStreamConfig("employees")
                        .setInitialRegionSize(1024)
                        .setValueClass(Employee.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataStream<Employee> stream = hz.getDataStream("employees");
        DataOutputStream<Employee> out = stream.newOutputStream();
        out.write("foo", new Employee(1, 1, 1));
        out.write("foo", new Employee(2, 2, 2));
        out.write("foo", new Employee(3, 3, 3));

        assertEquals(3, stream.asFrame().count());
        assertEquals(1, stream.asFrame().memoryInfo().regionsInUse());
    }

    @Test
    public void whenAppendMultiplePartitions() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "10")
                .addDataStreamConfig(
                        new DataStreamConfig("employees")
                                .setValueClass(Employee.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataStream<Employee> stream = hz.getDataStream("employees");
        DataOutputStream<Employee> out = stream.newOutputStream();

        long count = 10000;
        for (long k = 0; k < count; k++) {
            out.write(k, new Employee(1, 1, 1));
        }

        assertEquals(count, stream.asFrame().count());
        assertEquals(10, stream.asFrame().memoryInfo().regionsInUse());
    }

    @Test
    public void whenGrowingRequired() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "1")
                .addDataStreamConfig(
                        new DataStreamConfig("employees")
                                .setInitialRegionSize(1024)
                                .setValueClass(Employee.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataStream<Employee> stream = hz.getDataStream("employees");
        DataOutputStream<Employee> out = stream.newOutputStream();
        int itemCount = 100 * 1000;
        for (int k = 0; k < itemCount; k++) {
            out.write(k, new Employee(k, k, k));
        }

        assertEquals(itemCount, stream.asFrame().count());
        assertEquals(2, stream.asFrame().memoryInfo().regionsInUse());
        System.out.println(stream.asFrame().memoryInfo());
    }

    @Test
    public void whenMultipleRegionsNeeded() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "1")
                .addDataStreamConfig(
                        new DataStreamConfig("employees")
                                .setInitialRegionSize(1024)
                                .setMaxRegionsPerPartition(Integer.MAX_VALUE)
                                .setMaxRegionSize(1024)
                                .setValueClass(Employee.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataStream<Employee> stream = hz.getDataStream("employees");
        DataOutputStream<Employee> out = stream.newOutputStream();

        int itemCount = 100 * 1000;
        for (int k = 0; k < itemCount; k++) {
            out.write(k, new Employee(k, k, k));
        }

        assertEquals(itemCount, stream.asFrame().count());
        assertEquals(1961, stream.asFrame().memoryInfo().regionsInUse());
        System.out.println(stream.asFrame().memoryInfo());
    }

    @Test
    public void whenMultipleRegionsNeeded_andLimitOnSegment() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "1")
                .addDataStreamConfig(
                        new DataStreamConfig("employees")
                                .setInitialRegionSize(1024)
                                .setMaxRegionSize(1024)
                                .setMaxRegionsPerPartition(10)
                                .setValueClass(Employee.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataStream<Employee> stream = hz.getDataStream("employees");
        DataOutputStream<Employee> out = stream.newOutputStream();

        int itemCount = 100 * 1000;
        for (int k = 0; k < itemCount; k++) {
            out.write(k, new Employee(k, k, k));
        }

        //   assertEquals(itemCount, stream.count());
        assertEquals(10, stream.asFrame().memoryInfo().regionsInUse());
        System.out.println(stream.asFrame().memoryInfo());
    }

    //  @Test
    public void whenTenuring() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "1")
                .addDataStreamConfig(
                        new DataStreamConfig("employees")
                                .setInitialRegionSize(1024)
                                .setTenuringAgeMillis((int) SECONDS.toMillis(5))
                                //   .setMaxRegionSize(Long.MAX_VALUE)
                                .setValueClass(Employee.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataStream<Employee> stream = hz.getDataStream("employees");
        DataOutputStream<Employee> out = stream.newOutputStream();

        int itemCount = 100 * 1000;
        for (int k = 0; k < itemCount; k++) {
            out.write(k, new Employee(k, k, k));
            sleepSeconds(1);
            // System.out.println(stream.memoryUsage());
        }

        assertEquals(itemCount, stream.asFrame().count());
    }

}
