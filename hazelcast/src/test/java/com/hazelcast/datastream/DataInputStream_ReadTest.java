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
import org.junit.Ignore;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class DataInputStream_ReadTest extends HazelcastTestSupport {

    // todo
    @Ignore
    @Test
    public void test() throws InterruptedException {
        Config config = new Config()
                .addDataStreamConfig(
                        new DataStreamConfig("employees")
                                .setValueClass(Employee.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataStream<Employee> stream = hz.getDataStream("employees");

        int partitionId = getNodeEngineImpl(hz).getPartitionService().getPartitionId("foo");
        int produceCount = 1000;
        List<Employee> produced = new LinkedList<>();
        spawn(new Runnable() {
            @Override
            public void run() {
                DataOutputStream out = stream.newOutputStream();
                for (int k = 0; k < produceCount; k++) {
                    Employee record = new Employee(k, k, k);
                    produced.add(record);
                    sleepMillis(1);
                }
            }
        });

        Thread.sleep(1000);

        List<Employee> consumed = new LinkedList<>();
        DataInputStream<Employee> in = stream.newInputStream(partitionId, 0);
        do {
            Employee employee = in.read();
            consumed.add(employee);
        } while (consumed.size() != produceCount);


        assertEquals(produced, consumed);
    }
}
