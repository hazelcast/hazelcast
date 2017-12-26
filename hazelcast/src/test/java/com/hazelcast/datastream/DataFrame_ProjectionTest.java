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
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.spi.properties.GroupProperty.PARTITION_COUNT;
import static org.junit.Assert.assertEquals;

public class DataFrame_ProjectionTest extends HazelcastTestSupport {

    @Test(expected = CompletionException.class)
    public void whenBlob() {
        Config config = new Config()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "1")
                .addDataStreamConfig(
                        new DataStreamConfig("employees"));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataStream<Employee> stream = hz.getDataStream("employees");
        DataFrame<Employee> frame = stream.asFrame();
        frame.prepare(new ProjectionRecipe<>());
    }

    @Test
    public void compileIdentityProjection() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "1")
                .addDataStreamConfig(
                        new DataStreamConfig("employees")
                                .setValueClass(Employee.class));

        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances(config);

        DataStream<Employee> stream = cluster[0].getDataStream("employees");
        DataOutputStream<Employee> out = stream.newOutputStream();

        out.write((long) 0, new Employee(19, 10, 100));
        out.write((long) 1, new Employee(20, 20, 200));
        out.write((long) 2, new Employee(21, 30, 300));
        out.write((long) 3, new Employee(22, 40, 400));
        out.write((long) 3, new Employee(23, 50, 500));


        PreparedProjection<Employee> compiledPredicate = stream.asFrame().prepare(
                new ProjectionRecipe<Employee>(Employee.class, false, new SqlPredicate("age<=$age")));
        Map<String, Object> bindings = new HashMap<String, Object>();
        bindings.put("age", 20);

        Set<Employee> result = compiledPredicate.executePartitionThread(bindings, HashSet.class);
        System.out.println(result);
        assertEquals(2, result.size());
    }

    @Test
    public void compileProjectionAgeSalary() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "10")
                .addDataStreamConfig(
                        new DataStreamConfig("employees")
                                .setValueClass(Employee.class));

        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances(config);

        DataStream<Employee> stream = cluster[0].getDataStream("employees");
        DataOutputStream<Employee> out = stream.newOutputStream();
        for (int k = 0; k < 1000; k++) {
            out.write((long) k, new Employee(k, k, k));
        }

        PreparedProjection<AgeSalary> preparedProjection = stream.asFrame().prepare(
                new ProjectionRecipe<AgeSalary>(AgeSalary.class, true, new SqlPredicate("age==$age and iq==$iq and height>10")));
//        Map<String, Object> bindings = new HashMap<String, Object>();
//        bindings.put("age", 100);
//        bindings.put("iq", 100l);
//        compiledPredicate.execute(bindings);
    }

    @Test
    public void newDataStream() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "10")
                .addDataStreamConfig(
                        new DataStreamConfig("employees")
                                .setValueClass(Employee.class));

        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances(config);

        DataStream<Employee> stream = cluster[0].getDataStream("employees");
        DataOutputStream<Employee> out = stream.newOutputStream();
        for (int k = 0; k < 100; k++) {
            out.write((long) k, new Employee(k, k, k));
        }
        System.out.println("employees consumed memory:" + stream.asFrame().memoryInfo().bytesConsumed());

        PreparedProjection<AgeSalary> preparedProjection = stream.asFrame().prepare(
                new ProjectionRecipe<AgeSalary>(AgeSalary.class, true, new SqlPredicate("age<$age")));
        Map<String, Object> bindings = new HashMap<String, Object>();
        bindings.put("age", 50);

        DataStream ageSalaries = preparedProjection.newDataStream("ageSalary", bindings);
        assertEquals(stream.asFrame().count() / 2, ageSalaries.asFrame().count());
        System.out.println("agesalary memory:" + ageSalaries.asFrame().memoryInfo().bytesConsumed());
    }

    @Test
    public void newDataSetFromLargeInitialSet() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "1")
                .addDataStreamConfig(
                        new DataStreamConfig("employees")
                                .setMaxRegionsPerPartition(Integer.MAX_VALUE)
                                .setValueClass(Employee.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataStream<Employee> stream = hz.getDataStream("employees");
        DataOutputStream<Employee> out = stream.newOutputStream();

        int count = 50 * 1000 * 1000;
        stream.newOutputStream().fill(count, new EmployeeSupplier());

        PreparedProjection<AgeSalary> preparedProjection = stream.asFrame().prepare(
                new ProjectionRecipe<AgeSalary>(AgeSalary.class, true, new SqlPredicate("true")));
        Map<String, Object> bindings = new HashMap<String, Object>();

        DataStream ageSalaries = preparedProjection.newDataStream("ageSalary", bindings);
        DataStreamInfo memoryInfo = ageSalaries.asFrame().memoryInfo();
        System.out.println(memoryInfo);

        assertEquals(count * (INT_SIZE_IN_BYTES + INT_SIZE_IN_BYTES), memoryInfo.bytesConsumed());
        assertEquals(count, ageSalaries.asFrame().count());
    }

}
