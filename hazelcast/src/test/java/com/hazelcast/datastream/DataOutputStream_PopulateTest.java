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
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DataOutputStream_PopulateTest extends HazelcastTestSupport {

    @Test
    public void test() {
        Config config = new Config()
                .addDataStreamConfig(
                        new DataStreamConfig("employees"));

        HazelcastInstance hz = createHazelcastInstance(config);

        IMap<Long, Employee> employeesMap = hz.getMap("employeesMap");
        int itemCount = 10000;
        for (int k = 0; k < itemCount; k++) {
            employeesMap.put((long) k, new Employee(k, k, k));
        }

        DataStream<Employee> stream = hz.getDataStream("employees");
        stream.newOutputStream().populate(employeesMap);

        assertEquals(itemCount, stream.asFrame().count());
        System.out.println(stream.asFrame().memoryInfo());
    }
}
