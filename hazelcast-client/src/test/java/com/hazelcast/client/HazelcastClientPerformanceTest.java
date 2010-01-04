/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.client;

import static com.hazelcast.client.TestUtility.getHazelcastClient;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class HazelcastClientPerformanceTest {


    @Test
    public void putAndget100000RecordsWith1ClusterMember() {
        HazelcastClient hClient = getHazelcastClient();

        Map<String, String> map = hClient.getMap("putAndget100000RecordsWith1ClusterMember");
        putAndGet(map, 100000);
    }

    private void putAndGet(Map<String, String> map, int counter) {
        long beginTime = System.currentTimeMillis();
        for (int i = 0; i < counter; i++) {
            if (i % 10000 == 0) {
                System.out.println(i + ": " + (System.currentTimeMillis() - beginTime) + " ms");
            }
            map.put("key_" + i, String.valueOf(i));
        }
        System.out.println(System.currentTimeMillis() - beginTime);
        beginTime = System.currentTimeMillis();
        for (int i = 0; i < counter; i++) {
            if (i % 10000 == 0) {
                System.out.println(i + ": " + (System.currentTimeMillis() - beginTime) + " ms");
            }
            assertEquals(String.valueOf(i), map.get("key_" + i));
        }
//    	assertEquals(String.valueOf(i), map.get("key_"+i));
        System.out.println(System.currentTimeMillis() - beginTime);
    }

    @Test
    public void putFromMultipleThreads() throws InterruptedException {
        final HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        final AtomicInteger counter = new AtomicInteger(0);

        class Putter implements Runnable{
            volatile Boolean run = true;
            public void run() {
                HazelcastClient hClient = getHazelcastClient(h);
                while(run){
                    Map<String, String> clientMap = hClient.getMap("putFromMultipleThreads");
                    clientMap.put(String.valueOf(counter.incrementAndGet()), String.valueOf(counter.get()));
                }
            }
        };

        List<Putter> list = new ArrayList<Putter>();

        for(int i =0;i<10;i++){
            Putter p = new Putter();
            list.add(p);
            new Thread(p).start();
        }

        Thread.sleep(5000);

        for(Iterator<Putter> it = list.iterator(); it.hasNext();){
            Putter p = it.next();
            p.run = false;
        }

        Thread.sleep(100);
        assertEquals(counter.get(), h.getMap("putFromMultipleThreads").size());



    }
}
