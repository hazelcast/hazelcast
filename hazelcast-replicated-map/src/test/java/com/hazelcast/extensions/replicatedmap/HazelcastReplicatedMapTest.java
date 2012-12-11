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

package com.hazelcast.extensions.replicatedmap;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.impl.GroupProperties;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertTrue;

public class HazelcastReplicatedMapTest {

    @Test
    public void randomPut() throws InterruptedException {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_INITIAL_MIN_CLUSTER_SIZE, "2");
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
//        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        final HazelcastReplicatedMap<Integer, Integer> hzrm1 = new HazelcastReplicatedMap<Integer, Integer>(h1, "test");
//        final HazelcastReplicatedMap<Integer, Integer> hzrm2 = new HazelcastReplicatedMap<Integer, Integer>(h2, "test");
        Thread.sleep(10000);
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        int threadCount = 2;
        final CountDownLatch latch = new CountDownLatch(threadCount);
        for (int i = 0; i < threadCount; i++) {
            final int id = i;
            executorService.submit(new Runnable() {
                public void run() {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    HazelcastReplicatedMap<Integer, Integer> hzrm;
//                    if (id % 2 == 0)
                    hzrm = hzrm1;
//                    else
//                        hzrm = hzrm2;
                    Random random = new Random();
                    int count = 100;
                    for (int j = 0; j < count; j++) {
                        if (random.nextInt(count) % 2 == 0)
                            hzrm.put(random.nextInt(count), random.nextInt(count));
                        else
                            hzrm.remove(random.nextInt(count));
                    }
                    latch.countDown();
                }
            });
        }
        assertTrue(latch.await(threadCount * 10+10, TimeUnit.SECONDS));
        Thread.sleep(20000);
        List<Integer> keys1 = new ArrayList<Integer>(hzrm1.keySet());
//        List<Integer> keys2 = new ArrayList<Integer>(hzrm2.keySet());
        Collections.sort(keys1);
//        Collections.sort(keys2);
        for (int i : keys1) {
            System.out.print(i + ": " + hzrm1.get(i) + ", ");
        }
        System.out.println();
        
        Thread.sleep(1000000000);
//        for(int i: keys2){
//            System.out.print(i + ": " + hzrm2.get(i) + ", ");
//        }
//        assertEquals(hzrm1.size(), hzrm2.size());
//        for(Integer i: hzrm1.keySet()){
//            assertEquals(hzrm1.get(i), hzrm2.get(i));
//        }
//        for(Integer i: hzrm2.keySet()){
//            assertEquals(hzrm1.get(i), hzrm2.get(i));
//        }
    }
}
