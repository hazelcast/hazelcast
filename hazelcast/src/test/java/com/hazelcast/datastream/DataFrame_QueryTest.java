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
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletionException;

import static com.hazelcast.spi.properties.GroupProperty.PARTITION_COUNT;
import static org.junit.Assert.assertEquals;

public class DataFrame_QueryTest extends HazelcastTestSupport {

    @Test(expected = CompletionException.class)
    public void whenBlob() {
        Config config = new Config()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "1")
                .addDataStreamConfig(
                        new DataStreamConfig("employees"));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataStream<Employee> stream = hz.getDataStream("employees");
        DataFrame<Employee> frame = stream.asFrame();
        frame.prepare(TruePredicate.INSTANCE);
    }

    @Test
    public void compileQuery() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "10")
                .addDataStreamConfig(
                        new DataStreamConfig("employees")
                                .setValueClass(Employee.class));

        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances(config);

        DataStream<Employee> stream = cluster[0].getDataStream("employees");
        DataOutputStream<Employee> out = stream.newOutputStream();
        out.write(1L, new Employee(20, 100, 200));
        out.write(1L, new Employee(21, 101, 200));
        out.write(1L, new Employee(22, 103, 200));
        out.write(1L, new Employee(23, 100, 201));
        out.write(1L, new Employee(24, 100, 202));
        out.write(1L, new Employee(20, 100, 204));

        PreparedQuery<Employee> preparedQuery = stream.asFrame().prepare(new SqlPredicate("age==$age or iq=20 or salary=50"));
        Map<String, Object> bindings = new HashMap<String, Object>();
        bindings.put("age", 20);
        assertEquals(2, preparedQuery.execute(bindings).size());
    }

    @Test
    public void noResults() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "10")
                .addDataStreamConfig(
                        new DataStreamConfig("employees")
                                .setValueClass(Employee.class));

        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances(config);

        DataStream<Employee> stream = cluster[0].getDataStream("employees");
        DataOutputStream<Employee> out = stream.newOutputStream();
        out.write(1L, new Employee(20, 100, 200));
        out.write(1L, new Employee(21, 101, 200));
        out.write(1L, new Employee(22, 103, 200));
        out.write(1L, new Employee(23, 100, 201));
        out.write(1L, new Employee(24, 100, 202));
        out.write(1L, new Employee(20, 100, 204));

        PreparedQuery<Employee> preparedQuery = stream.asFrame().prepare(new SqlPredicate("age==$age"));
        Map<String, Object> bindings = new HashMap<String, Object>();
        bindings.put("age", 2000);
        assertEquals(0, preparedQuery.execute(bindings).size());
    }

    @Test
    public void compileQueryAll() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "10")
                .addDataStreamConfig(
                        new DataStreamConfig("employees")
                                .setValueClass(Employee.class));

        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances(config);

        DataStream<Employee> stream = cluster[0].getDataStream("employees");
        DataOutputStream<Employee> out = stream.newOutputStream();
        out.write(1L, new Employee(20, 100, 200));
        out.write(1L, new Employee(21, 101, 200));
        out.write(1L, new Employee(22, 103, 200));
        out.write(1L, new Employee(23, 100, 201));
        out.write(1L, new Employee(24, 100, 202));
        out.write(1L, new Employee(20, 100, 204));

        PreparedQuery<Employee> preparedQuery = stream.asFrame().prepare(new SqlPredicate("true"));
        Map<String, Object> bindings = new HashMap<String, Object>();
        assertEquals(6, preparedQuery.execute(bindings).size());
    }

    @Test
    public void queryMultiplePartitions() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "10")
                .addDataStreamConfig(
                        new DataStreamConfig("employees")
                                .setValueClass(Employee.class));

        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances(config);

        long count = 100000;
        int resultCount = 0;
        int queryAge = 20;
        DataStream<Employee> stream = cluster[0].getDataStream("employees");
        DataOutputStream<Employee> out = stream.newOutputStream();
        Random random = new Random();
        for (long k = 0; k < count; k++) {
            int age = random.nextInt(50);
            if (age == queryAge) {
                resultCount++;
            }
            out.write(k, new Employee(age, 100, 200));
        }

        PreparedQuery<Employee> preparedQuery = stream.asFrame().prepare(new SqlPredicate("age=$age"));
        Map<String, Object> bindings = new HashMap<String, Object>();
        bindings.put("age", queryAge);
        assertEquals(resultCount, preparedQuery.execute(bindings).size());
    }

    @Ignore
    @Test
    public void queryWithIndex_andNoBindParameter() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "1")
                .addDataStreamConfig(
                        new DataStreamConfig("employees")
                                .setValueClass(Employee.class)
                                .addIndexField("age"));

        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances(config);

        DataStream<Employee> stream = cluster[0].getDataStream("employees");
        DataOutputStream<Employee> out = stream.newOutputStream();
        out.write(1L, new Employee(20, 100, 200));

        PreparedQuery<Employee> preparedQuery = stream.asFrame().prepare(new SqlPredicate("age==10"));
        Map<String, Object> bindings = new HashMap<String, Object>();
        assertEquals(0, preparedQuery.execute(bindings).size());
    }

    @Ignore
    @Test
    public void queryWithIndex_andBindParameter() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "1")
                .addDataStreamConfig(
                        new DataStreamConfig("employees")
                                .setValueClass(Employee.class)
                                .addIndexField("age"));

        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances(config);

        DataStream<Employee> stream = cluster[0].getDataStream("employees");
        DataOutputStream<Employee> out = stream.newOutputStream();
        out.write(1L, new Employee(20, 100, 200));

        PreparedQuery<Employee> preparedQuery = stream.asFrame().prepare(new SqlPredicate("age==$age and iq=100"));
        Map<String, Object> bindings = new HashMap<String, Object>();
        bindings.put("age", 50);
        assertEquals(0, preparedQuery.execute(bindings).size());
    }
}
