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

public class SubscriptionTest extends HazelcastTestSupport {

    @Test
    public void test() throws InterruptedException {
        Config config = new Config()
                .addDataStreamConfig(
                        new DataStreamConfig("employees")
                                .setValueClass(Employee.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataStream<Employee> stream = hz.getDataStream("employees");

        spawn(new Runnable() {
            @Override
            public void run() {
                DataOutputStream out = stream.newOutputStream();
                for(int k=0;k<100;k++){
                //    out.write("foo",new Employee(k,k,k));
                    sleepSeconds(1);
                }
            }
        });

        DataInputStream<Employee> in = stream.newInputStream("foo",0);
        for(;;) {
            Employee employee = in.read();
            System.out.println(employee);
        }


        //
//        stream.newInputStream().add(new DataStreamConsumer<Employee>() {
//            @Override
//            public long consume(long offset, Employee record) {
//                System.out.println(record);
//                return 0;
//            }
//        });
    }
}
