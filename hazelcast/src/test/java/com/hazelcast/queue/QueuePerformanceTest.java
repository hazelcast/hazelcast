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

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import org.junit.Ignore;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

@Ignore("not a JUnit test")
public class QueuePerformanceTest {

    final AtomicLong totalOffer = new AtomicLong();
    final AtomicLong totalPoll = new AtomicLong();
    final AtomicLong totalPeek = new AtomicLong();
    final Random rnd = new Random();

    public static void main(String[] args) throws Exception {
//        System.setProperty("hazelcast.test.use.network","true");

        QueuePerformanceTest test = new QueuePerformanceTest();
        test.oneQueue();
//        test.manyQueue();
    }

    public void manyQueue() throws Exception {
        Config config = new Config();
        final HazelcastInstance ins = Hazelcast.newHazelcastInstance(config);
        final int threadCount = 10;
        final int queueCount = 1000;
        final IQueue[] queues = new IQueue[queueCount];
        for (int i=0; i<queueCount; i++){
            queues[i] = ins.getQueue("queue"+i);
        }
        System.err.println("starting threads");
        for (int i=0; i < threadCount; i++){
            new Thread(){
                public void run() {
                    while (true){
                        IQueue q = queues[rnd.nextInt(queueCount)];
                        int random = rnd.nextInt(100);
                        if(random > 65){
                            q.poll();
                            totalPoll.incrementAndGet();
                        }
                        else if(random > 30){
                            q.offer("item");
                            totalOffer.incrementAndGet();
                        }
                        else {
                            q.peek();
                            totalPeek.incrementAndGet();
                        }
                    }
                }
            }.start();
        }
        System.err.println("finished starting threads");

        while (true){
            long sleepTime = 10;
            Thread.sleep(sleepTime*1000);
            long totalOfferVal = totalOffer.getAndSet(0);
            long totalPollVal = totalPoll.getAndSet(0);
            long totalPeekVal = totalPeek.getAndSet(0);


            System.err.println("_______________________________________________________________________________________");
            System.err.println(" offer: " + totalOfferVal + ",\t poll: " + totalPollVal + ",\t peek: " + totalPeekVal);
            System.err.println(" speed: " + ((totalOfferVal+totalPollVal+totalPeekVal)/sleepTime));
            System.err.println("---------------------------------------------------------------------------------------");
            System.err.println("");
        }
    }


    public void oneQueue() throws Exception {
        Config config = new Config();
        final int threadCount = 1;
        final String name = "defQueue";
        final HazelcastInstance ins = Hazelcast.newHazelcastInstance(config);
        final IQueue<String> q = ins.getQueue(name);

        System.err.println("starting threads");
        for (int i=0; i < threadCount; i++){
            new Thread(){
                public void run() {
                    while (true){
                        int random = rnd.nextInt(100);
                        if(random > 54){
                            q.poll();
                            totalPoll.incrementAndGet();
                        }
                        else if(random > 8){
                            q.offer("item");
                            totalOffer.incrementAndGet();
                        }
                        else {
                            q.peek();
                            totalPeek.incrementAndGet();
                        }
                    }
                }
            }.start();
        }
        new Thread(){
            public void run() {
                while (true){
                    int size = q.size();
                    if (size > 50000){
                        System.err.println("cleaning a little");
                        for (int i=0; i < 10000; i++){
                            q.poll();
                            totalPoll.incrementAndGet();
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
            }
        }.start();
        System.err.println("finished starting threads");

        while (true){
            long sleepTime = 10;
            Thread.sleep(sleepTime*1000);
            long totalOfferVal = totalOffer.getAndSet(0);
            long totalPollVal = totalPoll.getAndSet(0);
            long totalPeekVal = totalPeek.getAndSet(0);


            System.err.println("_______________________________________________________________________________________");
            System.err.println(" offer: " + totalOfferVal + ",\t poll: " + totalPollVal + ",\t peek: " + totalPeekVal);
            System.err.println(" size: " + q.size() + " \t speed: " + ((totalOfferVal+totalPollVal+totalPeekVal)/sleepTime));
            System.err.println("---------------------------------------------------------------------------------------");
            System.err.println("");
        }
    }


}
