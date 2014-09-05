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

package com.hazelcast.queue;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import org.junit.Ignore;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Ignore("not a JUnit test")
public class LongRunningQueueTest {

    static final int minNode = 2;
    static final int maxNode = 7;
    static final String name = "defQueue";
    static final int threadCount = 5;
    static final int limit = 50000;
    static final Random rnd = new Random();
    static final AtomicLong totalOffer = new AtomicLong();
    static final AtomicLong totalPoll = new AtomicLong();
    static final Set<Server> servers = new HashSet<Server>(10);
    static volatile boolean done = false;

    private LongRunningQueueTest() {
    }


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
        Thread.sleep(10*1000);
        int queueSize = server.size();
        System.out.println("Total Put       : " + totalOffer.get());
        System.out.println("Total Remove    : " + totalPoll.get());
        System.out.println("Queue Size1     : " + queueSize);
        System.out.println("Expected Size   : " + (totalOffer.get() - totalPoll.get()));

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
            return ins.getQueue(name).size();
        }

        boolean stop(boolean shutdown) throws Exception {
            running = false;
            if(latch.await(30, TimeUnit.SECONDS)){
                Thread.sleep(2*1000);
                if (shutdown){
                    ins.shutdown();
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
            final IQueue q = ins.getQueue(name);
            for (int i=0; i < threadCount; i++){
                new Thread(){
                    public void run() {
                        while (running){
                            int random = rnd.nextInt(100);
                            if(random > 45){
                                if (q.offer("item")){
                                    totalOffer.incrementAndGet();
                                }
                            }
                            else {
                                if(q.poll() != null){
                                    totalPoll.incrementAndGet();
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
                        int size = q.size();
                        if (size > limit){
                            System.out.println("cleaning a little size: " + size);
                            for (int i=0; i < limit/2; i++){
                                if(q.poll() != null){
                                    totalPoll.incrementAndGet();
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
