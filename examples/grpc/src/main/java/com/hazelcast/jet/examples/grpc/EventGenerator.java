/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.examples.grpc;

import com.hazelcast.jet.examples.grpc.datamodel.Trade;
import com.hazelcast.map.IMap;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class EventGenerator extends Thread {

    private static final int PRODUCT_ID_BASE = 31;
    private static final int BROKER_ID_BASE = 21;
    private static final int PRODUCT_BROKER_COUNT = 4;

    private volatile boolean enabled;
    private volatile boolean keepRunning = true;

    private final IMap<Object, Trade> trades;

    EventGenerator(IMap<Object, Trade> trades) {
        this.trades = trades;
    }

    @Override
    public void run() {
        Random rnd = ThreadLocalRandom.current();
        int tradeId = 1;
        while (keepRunning) {
            LockSupport.parkNanos(MILLISECONDS.toNanos(50));
            if (!enabled) {
                continue;
            }
            Trade trad = new Trade(tradeId,
                    PRODUCT_ID_BASE + rnd.nextInt(PRODUCT_BROKER_COUNT),
                    BROKER_ID_BASE + rnd.nextInt(PRODUCT_BROKER_COUNT));
            trades.put(42, trad);
            tradeId++;
        }
    }

    void generateEventsForFiveSeconds() throws InterruptedException {
        enabled = true;
        System.out.println("\nGenerating trade events\n");
        Thread.sleep(5000);
        System.out.println("\nStopped trade events\n");
        enabled = false;
    }

    void shutdown() {
        keepRunning = false;
    }
}
