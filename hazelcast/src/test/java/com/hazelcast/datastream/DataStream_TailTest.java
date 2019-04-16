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
import static org.junit.Assert.assertEquals;

public class DataStream_TailTest extends HazelcastTestSupport {

    @Test
    public void whenEmpty() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "1")
                .addDataStreamConfig(new DataStreamConfig("employees")
                        .setInitialRegionSize(1024)
                        .setValueClass(Employee.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataStream<Employee> stream = hz.getDataStream("employees");
        assertEquals(0, stream.tail("f"));
    }

    @Test
    public void whenFewItemsAdded() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "1")
                .addDataStreamConfig(new DataStreamConfig("employees")
                        .setInitialRegionSize(1024 * 1024)
                        .setMaxRegionSize(1024 * 1024)
                        .setMaxRegionsPerPartition(3)
                        .setValueClass(Employee.class));

        HazelcastInstance hz = createHazelcastInstance(config);
        DataStream<Employee> stream = hz.getDataStream("employees");
        DataOutputStream out = stream.newOutputStream();
        for (int k = 0; k < 1000 * 1000; k++) {
            out.write(new Employee(1, 1, 1));
            assertEquals(20 * (k + 1), stream.tail("f"));
            if (k % 10000 == 0) {
                System.out.println("at:" + k);
            }
        }

    }
}
