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
import org.junit.Test;

import static com.hazelcast.spi.properties.GroupProperty.PARTITION_COUNT;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

public class AppendTest extends HazelcastTestSupport {

    @Test
    public void whenSimple() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "1")
                .addDataStreamConfig(new DataStreamConfig("employees")
                        .setInitialSegmentSize(1024)
                        .setValueClass(Employee.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataStream<Employee> stream = hz.getDataStream("employees");
        DataOutputStream<Employee> out = stream.newOutputStream();
        out.write(0, new Employee(1, 1, 1));

        assertEquals(1, stream.asFrame().count());
        assertEquals(1, stream.asFrame().memoryInfo().segmentsInUse());
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
        assertEquals(10, stream.asFrame().memoryInfo().segmentsInUse());
    }

    @Test
    public void whenGrowingRequired() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "1")
                .addDataStreamConfig(
                        new DataStreamConfig("employees")
                                .setInitialSegmentSize(1024)
                                .setValueClass(Employee.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataStream<Employee> stream = hz.getDataStream("employees");
        DataOutputStream<Employee> out = stream.newOutputStream();
        int itemCount = 100 * 1000;
        for (int k = 0; k < itemCount; k++) {
            out.write(k, new Employee(k, k, k));
        }

        assertEquals(itemCount, stream.asFrame().count());
        assertEquals(2, stream.asFrame().memoryInfo().segmentsInUse());
        System.out.println(stream.asFrame().memoryInfo());
    }

    @Test
    public void whenMultipleSegmentsNeeded() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "1")
                .addDataStreamConfig(
                        new DataStreamConfig("employees")
                                .setInitialSegmentSize(1024)
                                .setSegmentsPerPartition(Integer.MAX_VALUE)
                                .setMaxSegmentSize(1024)
                                .setValueClass(Employee.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataStream<Employee> stream = hz.getDataStream("employees");
        DataOutputStream<Employee> out = stream.newOutputStream();

        int itemCount = 100 * 1000;
        for (int k = 0; k < itemCount; k++) {
            out.write(k, new Employee(k, k, k));
        }

        assertEquals(itemCount, stream.asFrame().count());
        assertEquals(1961, stream.asFrame().memoryInfo().segmentsInUse());
        System.out.println(stream.asFrame().memoryInfo());
    }

    @Test
    public void whenMultipleSegmentsNeeded_andLimitOnSegment() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "1")
                .addDataStreamConfig(
                        new DataStreamConfig("employees")
                                .setInitialSegmentSize(1024)
                                .setMaxSegmentSize(1024)
                                .setSegmentsPerPartition(10)
                                .setValueClass(Employee.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataStream<Employee> stream = hz.getDataStream("employees");
        DataOutputStream<Employee> out = stream.newOutputStream();

        int itemCount = 100 * 1000;
        for (int k = 0; k < itemCount; k++) {
            out.write(k, new Employee(k, k, k));
        }

        //   assertEquals(itemCount, stream.count());
        assertEquals(10, stream.asFrame().memoryInfo().segmentsInUse());
        System.out.println(stream.asFrame().memoryInfo());
    }

    //  @Test
    public void whenTenuring() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "1")
                .addDataStreamConfig(
                        new DataStreamConfig("employees")
                                .setInitialSegmentSize(1024)
                                .setTenuringAgeMillis((int) SECONDS.toMillis(5))
                                //   .setMaxSegmentSize(Long.MAX_VALUE)
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
