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

package com.hazelcast.examples;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IMap;
import com.hazelcast.impl.MapOperationStats;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SimpleMapTest {

    public static final int ENTRY_COUNT = 10 * 1000;
    public static final int VALUE_SIZE = 1000;
    public static final int STATS_SECONDS = 10;

    public static void main(String[] args) {
        int threadCount = 40;
        ExecutorService es = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            es.submit(new Runnable() {
                public void run() {
                    IMap<String, byte[]> map = Hazelcast.getMap("default");
                    while (true) {
                        int key = (int) (Math.random() * ENTRY_COUNT);
                        int operation = ((int) (Math.random() * 100)) % 10;
                        if (operation < 4) {
                            map.put(String.valueOf(key), new byte[VALUE_SIZE]);
                        } else if (operation < 8) {
                            map.get(String.valueOf(key));
                        } else {
                            map.remove(String.valueOf(key));
                        }
                    }
                }
            });
        }
        Executors.newSingleThreadExecutor().submit(new Runnable() {
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(STATS_SECONDS * 1000);
                        System.out.println("cluster size:"
                                + Hazelcast.getCluster().getMembers().size());
                        IMap<String, byte[]> map = Hazelcast.getMap("default");
                        MapOperationStats mapOpStats = map.getLocalMapStats().getOperationStats();
                        long period = ((mapOpStats.getPeriodEnd() - mapOpStats.getPeriodStart()) / 1000);
                        if(period==0){
                            continue;
                        }
                        System.out.println(mapOpStats);
                        System.out.println("Operations per Second : " + mapOpStats.total()
                                / period);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });
    }
}
