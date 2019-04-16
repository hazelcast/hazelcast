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
import com.hazelcast.datastream.impl.DSProxy;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class IteratorTest extends HazelcastTestSupport {

    @Test
    public void iterator() {
        Config config = new Config()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "1")
                .addDataStreamConfig(
                        new DataStreamConfig("employees")
                                .setInitialRegionSize(256)
                                .setMaxRegionSize(256)
                                .setMaxRegionsPerPartition(Integer.MAX_VALUE)
                                .setValueClass(Employee.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        List<Employee> employeeList = new ArrayList<>();
        EmployeeSupplier supplier = new EmployeeSupplier();
        DSProxy<Employee> stream = (DSProxy) hz.getDataStream("employees");
        DataOutputStream<Employee> out = stream.newOutputStream();

        for (long k = 0; k < 1000; k++) {
            Employee employee = supplier.get();
            employeeList.add(employee);
            out.write(0, employee);
        }

        stream.asFrame().freeze();

        Iterator<Employee> it = stream.iterator(0);

        List actual = new LinkedList();
        it.forEachRemaining(actual::add);

        assertEquals(employeeList.size(), actual.size());
        for (int k = 0; k < employeeList.size(); k++) {
            assertEquals("at index:" + k, employeeList.get(k), actual.get(k));
        }
    }
}
