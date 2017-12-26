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

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.aggregation.impl.LongAverageAggregator;
import com.hazelcast.aggregation.impl.MaxAggregator;
import com.hazelcast.config.Config;
import com.hazelcast.config.DataStreamConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletionException;

import static com.hazelcast.memory.MemoryUnit.KILOBYTES;
import static org.junit.Assert.assertEquals;

public class DataFrame_AggregationTest extends HazelcastTestSupport {

    @Test(expected = CompletionException.class)
    public void whenBlob() {
        Config config = new Config()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "1")
                .addDataStreamConfig(
                        new DataStreamConfig("employees"));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataStream<Employee> stream = hz.getDataStream("employees");
        DataFrame<Employee> frame = stream.asFrame();
        frame.prepare(new AggregationRecipe<>());
    }

    @Test
    public void whenForkJoinUsed() {
        Config config = new Config()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "1")
                .addDataStreamConfig(
                        new DataStreamConfig("employees")
                                .setMaxRegionsPerPartition(Integer.MAX_VALUE)
                                .setMaxRegionSize(16, KILOBYTES)
                                .setValueClass(Employee.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataStream<Employee> stream = hz.getDataStream("employees");
        DataOutputStream<Employee> out = stream.newOutputStream();
        int itemCount = 100 * 1000;
        long maxAge = Long.MIN_VALUE;
        Random random = new Random();
        for (int k = 0; k < itemCount; k++) {
            int age = random.nextInt(10000000);
            maxAge = Math.max(maxAge, age);
            out.write((long) k, new Employee(age, k, k));
        }

        Aggregator aggregator = new MaxAggregator();
        PreparedAggregation preparedAggregation = stream.asFrame().prepare(
                new AggregationRecipe<Long, Age>(Age.class, aggregator, new SqlPredicate("true")));
        Map<String, Object> bindings = new HashMap<String, Object>();
        // bindings.put("age", 200);
        //bindings.put("iq", 100l);
        Object result = preparedAggregation.executeForkJoin(bindings);
        assertEquals(maxAge, result);
    }

    @Test
    public void maxAgeAggregationMultiplePartitions() {
        Config config = new Config()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "10")
                .addDataStreamConfig(
                        new DataStreamConfig("employees")
                                .setValueClass(Employee.class));

        HazelcastInstance[] cluster = createHazelcastInstanceFactory(1).newInstances(config);

        DataStream<Employee> dataStream = cluster[0].getDataStream("employees");
        DataOutputStream<Employee> out = dataStream.newOutputStream();

        long maxAge = Long.MIN_VALUE;
        Random random = new Random();
        for (long k = 0; k < 1000; k++) {
            int age = random.nextInt(100000);
            maxAge = Math.max(maxAge, age);
            out.write(k, new Employee(age, (int) k, (int) k));
        }

        Aggregator aggregator = new MaxAggregator();

        PreparedAggregation preparedAggregation = dataStream.asFrame().prepare(
                new AggregationRecipe<Long, Age>(Age.class, aggregator, new SqlPredicate("true")));
        Map<String, Object> bindings = new HashMap<String, Object>();
        // bindings.put("age", 200);
        //bindings.put("iq", 100l);
        Object result = preparedAggregation.executePartitionThread(bindings);

        System.out.println("max inserted age:" + maxAge);
        assertEquals(maxAge, result);
    }

    @Test
    public void maxAgeAggregationSinglePartition() {
        Config config = new Config()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "1")
                .addDataStreamConfig(
                        new DataStreamConfig("employees")
                                .setValueClass(Employee.class));

        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances(config);

        DataStream<Employee> stream = cluster[0].getDataStream("employees");
        DataOutputStream<Employee> out = stream.newOutputStream();
        long maxAge = Long.MIN_VALUE;
        Random random = new Random();
        for (int k = 0; k < 1000; k++) {
            int age = random.nextInt(100000);
            maxAge = Math.max(maxAge, age);
            out.write((long) k, new Employee(age, k, k));
        }

        Aggregator aggregator = new MaxAggregator();

        PreparedAggregation preparedAggregation = stream.asFrame().prepare(
                new AggregationRecipe<Long, Age>(Age.class, aggregator, new SqlPredicate("true")));
        Map<String, Object> bindings = new HashMap<String, Object>();
        // bindings.put("age", 200);
        //bindings.put("iq", 100l);
        Object result = preparedAggregation.executePartitionThread(bindings);
        assertEquals(maxAge, result);
    }

    @Test
    public void averageAgeAggregation() {
        Config config = new Config()
                .addDataStreamConfig(
                        new DataStreamConfig("employees")
                                .setValueClass(Employee.class));

        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances(config);

        DataStream<Employee> stream = cluster[0].getDataStream("employees");
        DataOutputStream<Employee> out = stream.newOutputStream();
        double totalAge = 0;
        Random random = new Random();
        int count = 1000;
        for (int k = 0; k < count; k++) {
            int age = random.nextInt(100000);
            totalAge += age;

            out.write((long) k, new Employee(age, k, k));
        }

        Aggregator aggregator = new LongAverageAggregator();

        PreparedAggregation preparedAggregation = stream.asFrame().prepare(
                new AggregationRecipe<Long, Age>(Age.class, aggregator, new SqlPredicate("true")));
        Map<String, Object> bindings = new HashMap<String, Object>();

        Double result = (Double) preparedAggregation.executePartitionThread(bindings);

        assertEquals(totalAge / count, (double) result, 0.1);
    }
}
