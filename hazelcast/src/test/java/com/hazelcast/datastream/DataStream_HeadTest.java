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

public class DataStream_HeadTest extends HazelcastTestSupport {

    @Test
    public void whenEmpty() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "1")
                .addDataStreamConfig(new DataStreamConfig("employees")
                        .setInitialRegionSize(1024)
                        .setValueClass(Employee.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataStream<Employee> stream = hz.getDataStream("employees");
        assertEquals(0, stream.head("f"));
    }

    @Test
    public void whenFewItemsAdded() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "1")
                .addDataStreamConfig(new DataStreamConfig("employees")
                        .setInitialRegionSize(1024)
                        .setValueClass(Employee.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataStream<Employee> stream = hz.getDataStream("employees");
        stream.newOutputStream().write("f", new Employee(1, 1, 1));
        assertEquals(0, stream.head("f"));
    }

    @Test
    public void whenManyItemsAdded() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "1")
                .addDataStreamConfig(new DataStreamConfig("employees")
                        .setInitialRegionSize(8 * 1024)
                        .setMaxRegionSize(8 * 1024)
                        .setMaxRegionsPerPartition(3)
                        //.setTenuringAge(10, TimeUnit.SECONDS)
                        .setValueClass(Employee.class));

        HazelcastInstance hz = createHazelcastInstance(config);
        DataStream<Employee> stream = hz.getDataStream("employees");
        DataOutputStream out = stream.newOutputStream();
        long prev = -1;
        for (int k = 0; k < 10000; k++) {
            out.write(new Employee(1, 1, 1));
            long head = stream.head("f");
            if (head != prev) {
                long tail = stream.tail("f");
                System.out.println("at:" + k + " head:" + head + " tail:" + tail + " diff:" + (tail - head));
                prev = head;
            }
//            assertEquals(20 * (k+1), head);
//            head
//            if(k%10000==0){
//
//            }
        }

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println(stream.head("f"));
    }
}
