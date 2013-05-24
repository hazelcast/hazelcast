/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.instance.GroupProperties;
import org.junit.Ignore;

import java.util.concurrent.CountDownLatch;

/**
 * @ali 5/21/13
 */
@Ignore("not a JUnit test")
public class ClientConnectionTest {

    static {
        System.setProperty("hazelcast.version.check.enabled", "false");
        System.setProperty("hazelcast.socket.bind.any", "false");
        System.setProperty("hazelcast.local.localAddress", "127.0.0.1");
        System.setProperty("java.net.preferIPv4Stack", "true");
        System.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
//        System.setProperty("java.net.preferIPv6Addresses", "true");
//        System.setProperty("hazelcast.prefer.ipv4.stack", "false");
    }

    static HazelcastInstance server1;
    static HazelcastInstance server2;
    static HazelcastInstance client;
    static IQueue<String> q;
    static final String name = "test";

    public static void main(String[] args) throws InterruptedException {
        server1 = Hazelcast.newHazelcastInstance();
        server2 = Hazelcast.newHazelcastInstance();

        client = HazelcastClient.newHazelcastClient(null);

        q = client.getQueue(name);
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(){
            public void run() {
                for (int i=0; i<100; i++){
                    if(q.offer("item"+i)){
                        System.err.println("offered item" + i);
                    }
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                latch.countDown();
                System.err.println("-------------done");
            }
        }.start();
        Thread.sleep(1000);
        server2.getLifecycleService().shutdown();

        latch.await();

        q.offer("item");


        System.err.println("size : " + q.size());
        client.getLifecycleService().shutdown();

        Hazelcast.shutdownAll();
    }


}
