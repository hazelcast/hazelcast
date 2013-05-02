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

package com.hazelcast.map;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @ali 4/30/13
 */
public class LongRunningMapTest {
    static final int minNode = 2;
    static final int maxNode = 7;
    static final String name = "defMap";
    static final int threadCount = 5;
    static final int limit = 50000;
    static final Random rnd = new Random(System.currentTimeMillis());
    static final AtomicLong totalPut = new AtomicLong();
    static final AtomicLong totalRemove = new AtomicLong();
    static final Set<Server> servers = new HashSet<Server>(10);
    static volatile boolean done = false;


    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.local.localAddress", "127.0.0.1");
        test1();
    }

    public static void test1() throws Exception {

        new Thread(){
            public void run() {
                try {
                    byte[] data = new byte[1024];
                    int a = System.in.read(data);
                    while (a != -1){
                        String s = new String(data, 0, a);
                        if (s.startsWith("done")){
                            System.out.println("finishing test");
                            done = true;
                            break;
                        }
                        a = System.in.read(data);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }.start();



        while (!done){
            addRemoveInstance();
            Thread.sleep(10 * 1000);
        }
        System.out.println("stopping servers");
        Iterator<Server> iter = servers.iterator();
        Server server = null;
        while (iter.hasNext()){
            server = iter.next();
            server.stop(false);
        }
        int mapSize1 = server.size();
        Thread.sleep(20*1000);
        int mapSize2 = server.size();
        System.out.println("Total Put       : " + totalPut.get());
        System.out.println("Total Remove    : " + totalRemove.get());
        System.out.println("Map Size1       : " + mapSize1);
        System.out.println("Map Size1       : " + mapSize2);
        System.out.println("Expected Size   : " + (totalPut.get() - totalRemove.get()));

//        Hazelcast.shutdownAll();
    }


    private static void addRemoveInstance() throws Exception {
        if (done){
            return;
        }
        boolean create = rnd.nextInt(100) % 2 == 0;
        int size = servers.size();
        if (size < minNode || (size != maxNode && create)){
            System.out.println("size: " + size + ", creating instance");
            Server server = new Server();
            server.run();
            servers.add(server);

        }
        else if (size == maxNode || !create){
            System.out.println("size: " + size + ", removing instance");
            Iterator<Server> iter = servers.iterator();
            Server server = iter.next();
            iter.remove();
            if(!server.stop(true)){
                throw new Exception("server did not stop properly");
            }
        }
    }

    static class Server {

        final HazelcastInstance ins;
        volatile boolean running = true;
        final CountDownLatch latch = new CountDownLatch(threadCount+1);

        Server() {
            ins = Hazelcast.newHazelcastInstance();
        }

        int size(){
            final IMap m = ins.getMap(name);
            return m.size();
        }

        boolean stop(boolean shutdown) throws Exception {
            running = false;
            if(latch.await(100, TimeUnit.SECONDS)){
                Thread.sleep(2*1000);
                if (shutdown){
                    ins.getLifecycleService().shutdown();
                }
                System.out.println("successfully stopped");
                return true;
            }
            else {
                System.out.println("latch not finished properly");
                return false;
            }
        }

        public void run() throws Exception {
            final IMap m = ins.getMap(name);
            for (int i=0; i < threadCount; i++){
                new Thread(){
                    public void run() {
                        while (running){
                            int random = rnd.nextInt(100);
                            int key = rnd.nextInt(limit*2);
                            if(random > 30){
                                if(m.put("key"+key, "value"+key) == null){
                                    totalPut.incrementAndGet();
                                }
                            }
                            else {
                                if(m.remove("key"+key) != null){
                                    totalRemove.incrementAndGet();
                                }
                            }
                        }
                        latch.countDown();
                    }
                }.start();
            }

            new Thread(){
                public void run() {
                    while (running){
                        int size = m.size();
                        if (size > limit){
                            System.out.println("cleaning a little size: " + size);
                            for (int i=0; i < limit; i++){
                                int key = rnd.nextInt(limit*2);
                                if(m.remove("key" + key) != null){
                                    totalRemove.incrementAndGet();
                                }
                            }
                        }
                        else{
                            try {
                                Thread.sleep(10*1000);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                    latch.countDown();
                }
            }.start();

        }
    }
}
