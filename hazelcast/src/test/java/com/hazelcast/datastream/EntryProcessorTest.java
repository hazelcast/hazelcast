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
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

import static com.hazelcast.spi.properties.GroupProperty.PARTITION_COUNT;
import static org.junit.Assert.assertEquals;

public class EntryProcessorTest extends HazelcastTestSupport {

    @Test
    public void testFieldMutator() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "10")
                .addDataStreamConfig(
                        new DataStreamConfig("employees")
                                .setValueClass(Employee.class));

        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances(config);

        DataStream<Employee> employees = cluster[0].getDataStream("employees");
        DataStreamPublisher<Employee> publisher = employees.createPublisher();

        int initialAge = 20;
        for (int k = 0; k < 50; k++) {
            publisher.publish((long) k, new Employee(20, k, k));
        }
        System.out.println("employees consumed memory:" + employees.asFrame().memoryInfo().consumedBytes());

        PreparedEntryProcessor<AgeSalary> preparedEntryProcessor = employees.asFrame().prepare(
                new EntryProcessorRecipe(new SqlPredicate("age=20"), new MultiplyMutator("age", 10)));

        preparedEntryProcessor.execute(new HashMap<>());

        PreparedQuery<Employee> preparedQuery = employees.asFrame().prepare(new SqlPredicate("true"));
        List<Employee> employeeList = preparedQuery.execute(new HashMap<>());
        for (Employee employee : employeeList) {
            assertEquals(initialAge * 2, employee.age);
        }
    }

    @Test
    public void testRecordMutator() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "10")
                .addDataStreamConfig(
                        new DataStreamConfig("employees")
                                .setValueClass(Employee.class));

        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances(config);

        DataStream<Employee> employees = cluster[0].getDataStream("employees");
        DataStreamPublisher<Employee> publisher = employees.createPublisher();
        int initialAge = 20;
        for (int k = 0; k < 50; k++) {
            publisher.publish((long) k, new Employee(20, k, k));
        }

        PreparedEntryProcessor<AgeSalary> preparedEntryProcessor = employees.asFrame().prepare(
                new EntryProcessorRecipe(new SqlPredicate("true"), new IncreaseAgeSalary()));

        preparedEntryProcessor.execute(new HashMap<>());

        PreparedQuery<Employee> preparedQuery = employees.asFrame().prepare(new SqlPredicate("true"));
        List<Employee> employeeList = preparedQuery.execute(new HashMap<>());
        for (Employee employee : employeeList) {
            assertEquals(initialAge * 2, employee.age);
        }
    }

}
