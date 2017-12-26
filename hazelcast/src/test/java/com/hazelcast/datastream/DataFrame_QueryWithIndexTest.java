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
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class DataFrame_QueryWithIndexTest extends HazelcastTestSupport {

    @Ignore
    @Test
    public void compileQuery() {
        Config config = new Config()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "10")
                .addDataStreamConfig(
                        new DataStreamConfig("employees")
                                .setValueClass(Employee.class)
                                .addIndexField("age"));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataStream<Employee> stream = hz.getDataStream("employees");
        DataOutputStream<Employee> out = stream.newOutputStream();

        out.write(1L, new Employee(20, 100, 200));
        out.write(1L, new Employee(20, 101, 200));
        out.write(1L, new Employee(20, 103, 200));
        out.write(1L, new Employee(21, 100, 201));
        out.write(1L, new Employee(22, 100, 202));

        PreparedQuery<Employee> preparedQuery = stream.asFrame().prepare(new SqlPredicate("age==20"));
        Map<String, Object> bindings = new HashMap<String, Object>();
        bindings.put("age", 20);
        assertEquals(3, preparedQuery.execute(bindings).size());
    }
}
