/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.Ignore;

/**
 * @ali 5/21/13
 */
@Ignore("not a JUnit test")
public class ClientConnectionTest {

    static HazelcastInstance server1;
    static HazelcastInstance server2;
    static HazelcastInstance client;
    static final String name = "test";

    public static void main(String[] args) throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setConnectionAttemptLimit(30);
        clientConfig.setRedoOperation(true);
        clientConfig.setAttemptPeriod(4000);

        server1 = Hazelcast.newHazelcastInstance();


        final HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        System.err.println("c: " + client);

        Thread.sleep(2000);

        System.err.println("--------------------------");
        System.err.println("--------------------------");
        System.err.println("--------------------------");
        System.err.println("--------------------------");
        server1.getLifecycleService().shutdown();
        System.err.println("shutdown");
        Thread.sleep(20000);



        server1 = Hazelcast.newHazelcastInstance();

        System.err.println("offering");
        client.getQueue(name).offer("item");
        System.err.println("offered");
        Object item = client.getQueue(name).poll();
        System.err.println("item:" + item);
    }


}
