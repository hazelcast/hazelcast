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

package com.hazelcast.queue;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.instance.StaticNodeFactory;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @ali 2/22/13
 */
public class QueuePerformanceTest {

    final AtomicLong totalOffer = new AtomicLong();
    final AtomicLong totalPoll = new AtomicLong();
    final Random rnd = new Random(System.currentTimeMillis());
    private HazelcastInstance[] instances;

    public static void main(String[] args) throws Exception {
//        System.setProperty("hazelcast.test.use.network","true");

        QueuePerformanceTest test = new QueuePerformanceTest();
        test.oneQueue();
    }

    public void oneQueue() throws Exception {
        Config config = new Config();
        final int insCount = 4;
        final int threadCount = 10;
        final String name = "defQueue";
        instances = StaticNodeFactory.newInstances(config, insCount);

//        Thread.sleep(3000);
//        getQueue(name).size();
//        Thread.sleep(3000);
//        getQueue(name).size();

        System.err.println("starting threads");
        for (int i=0; i < threadCount; i++){
            new Thread(){
                public void run() {
                    while (true){
                        if(rnd.nextInt(100) > 41){
                            getQueue(name).poll();
                            totalPoll.incrementAndGet();
                        }
                        else {
                            getQueue(name).offer("item");
                            totalOffer.incrementAndGet();
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


            System.err.println("_______________________________________________________________________________________");
            System.err.println(" offer: " + totalOfferVal + ",\t poll: " + totalPollVal);
            System.err.println(" size: " + getQueue(name).size() + " \t speed: " + ((totalOfferVal+totalPollVal)/sleepTime));
            System.err.println("---------------------------------------------------------------------------------------");
            System.err.println("");
        }
    }

    private IQueue getQueue(String name){
        return instances[rnd.nextInt(instances.length)].getQueue(name);
    }

}
