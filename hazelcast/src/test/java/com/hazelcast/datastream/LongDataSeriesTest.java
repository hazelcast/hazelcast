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

import java.util.Random;

import static com.hazelcast.spi.properties.GroupProperty.PARTITION_COUNT;
import static org.junit.Assert.assertEquals;

public class LongDataSeriesTest extends HazelcastTestSupport {

    @Test
    public void average() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "10")
                .addDataStreamConfig(
                        new DataStreamConfig("employees")
                                .setValueClass(Employee.class));
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(1).newInstances(config);

        DataStream<Employee> stream = cluster[0].getDataStream("employees");
        DataOutputStream<Employee> out = stream.newOutputStream();
        out.write(1, new Employee(10, 0, 0));
        out.write(1, new Employee(20, 0, 0));
        out.write(1, new Employee(30, 0, 0));

        LongDataSeries ageDS = stream.asFrame().getLongDataSeries("age");
        assertEquals(20, ageDS.average(), 0.1f);
    }

    @Test
    public void max() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "10")
                .addDataStreamConfig(
                        new DataStreamConfig("employees")
                                .setValueClass(Employee.class));

        HazelcastInstance[] cluster = createHazelcastInstanceFactory(1).newInstances(config);

        DataStream<Employee> stream = cluster[0].getDataStream("employees");
        DataOutputStream<Employee> out = stream.newOutputStream();

        long maxAge = Long.MIN_VALUE;
        Random random = new Random();
        for (long k = 0; k < 1000; k++) {
            int age = random.nextInt(100000);
            maxAge = Math.max(maxAge, age);
            out.write(k, new Employee(age, (int) k, (int) k));
        }

        LongDataSeries ageDS = stream.asFrame().getLongDataSeries("age");
        assertEquals(maxAge, ageDS.max());
    }

    @Test
    public void min() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "10")
                .addDataStreamConfig(
                        new DataStreamConfig("employees")
                                .setValueClass(Employee.class));

        HazelcastInstance[] cluster = createHazelcastInstanceFactory(1).newInstances(config);

        DataStream<Employee> stream = cluster[0].getDataStream("employees");
        DataOutputStream<Employee> out = stream.newOutputStream();

        long minAge = Long.MAX_VALUE;
        Random random = new Random();
        for (long k = 0; k < 1000; k++) {
            int age = 10000 + random.nextInt(100000);
            minAge = Math.min(minAge, age);
            out.write(k, new Employee(age, (int) k, (int) k));
        }

        LongDataSeries ageDS = stream.asFrame().getLongDataSeries("age");
        assertEquals(minAge, ageDS.min());
    }

    @Test
    public void sum() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "10")
                .addDataStreamConfig(
                        new DataStreamConfig("employees")
                                .setValueClass(Employee.class));

        HazelcastInstance[] cluster = createHazelcastInstanceFactory(1).newInstances(config);

        DataStream<Employee> stream = cluster[0].getDataStream("employees");
        DataOutputStream<Employee> out = stream.newOutputStream();

        long sum = 0;
        Random random = new Random();
        for (long k = 0; k < 1000; k++) {
            int age = 10000 + random.nextInt(100000);
            sum += age;
            out.write(k, new Employee(age, (int) k, (int) k));
        }

        LongDataSeries ageDS = stream.asFrame().getLongDataSeries("age");
        assertEquals(sum, ageDS.sum());
    }
}
