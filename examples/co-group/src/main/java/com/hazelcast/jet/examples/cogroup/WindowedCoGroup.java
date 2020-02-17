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

package com.hazelcast.jet.examples.cogroup;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.examples.cogroup.datamodel.AddToCart;
import com.hazelcast.jet.examples.cogroup.datamodel.PageVisit;
import com.hazelcast.jet.examples.cogroup.datamodel.Payment;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StageWithKeyAndWindow;
import com.hazelcast.jet.pipeline.StreamStageWithKey;
import com.hazelcast.jet.pipeline.WindowGroupAggregateBuilder;
import com.hazelcast.map.IMap;

import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.jet.Util.mapEventNewValue;
import static com.hazelcast.jet.Util.mapPutEvents;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.aggregate.AggregateOperations.toList;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@SuppressWarnings("Convert2MethodRef") // https://bugs.openjdk.java.net/browse/JDK-8154236
public final class WindowedCoGroup {
    private static final String TOPIC = "topic";
    private static final String PAGE_VISIT = "pageVisit";
    private static final String ADD_TO_CART = "addToCart";
    private static final String PAYMENT = "payment";

    public static void main(String[] args) throws Exception {
        // All IMap partitions must receive updates for the watermark to advance
        // correctly. Since we use just a handful of keys in this sample, we set a
        // low partition count.
        System.setProperty("hazelcast.partition.count", "1");

        JetInstance jet = Jet.bootstrappedInstance();
        ProducerTask producer = new ProducerTask(jet);

        try {
            // uncomment one of these
//            Pipeline p = aggregate();
//            Pipeline p = groupAndAggregate();
//            Pipeline p = coGroupAndAggregate();
            Pipeline p = coGroupWithBuilder();
            Job job = jet.newJob(p);
            Thread.sleep(5000);
            producer.stop();
            job.cancel();
        } finally {
            producer.stop();
            Jet.shutdownAll();
        }
    }

    private static Pipeline aggregate() {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.<PageVisit, Integer, PageVisit>mapJournal(PAGE_VISIT,
                START_FROM_OLDEST, mapEventNewValue(), mapPutEvents()))
         .withTimestamps(pv -> pv.timestamp(), 100)
         .window(sliding(10, 1))
         .aggregate(counting())
         .writeTo(Sinks.logger());
        return p;
    }

    private static Pipeline groupAndAggregate() {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.<PageVisit, Integer, PageVisit>mapJournal(PAGE_VISIT,
                START_FROM_OLDEST, mapEventNewValue(), mapPutEvents()))
         .withTimestamps(pv -> pv.timestamp(), 100)
         .window(sliding(10, 1))
         .groupingKey(pv -> pv.userId())
         .aggregate(toList())
         .writeTo(Sinks.logger());
        return p;
    }

    private static Pipeline coGroupAndAggregate() {
        Pipeline p = Pipeline.create();

        StreamStageWithKey<PageVisit, Integer> pageVisits = p
                .readFrom(Sources.<PageVisit, Integer, PageVisit>mapJournal(PAGE_VISIT,
                        START_FROM_OLDEST, mapEventNewValue(), mapPutEvents()))
                .withTimestamps(pv -> pv.timestamp(), 100)
                .groupingKey(pv -> pv.userId());
        StreamStageWithKey<Payment, Integer> payments = p
                .readFrom(Sources.<Payment, Integer, Payment>mapJournal(PAYMENT,
                        START_FROM_OLDEST, mapEventNewValue(), mapPutEvents()))
                .withTimestamps(pm -> pm.timestamp(), 100)
                .groupingKey(pm -> pm.userId());
        StreamStageWithKey<AddToCart, Integer> addToCarts = p
                .readFrom(Sources.<AddToCart, Integer, AddToCart>mapJournal(ADD_TO_CART,
                        START_FROM_OLDEST, mapEventNewValue(), mapPutEvents()))
                .withTimestamps(atc -> atc.timestamp(), 100)
                .groupingKey(atc -> atc.userId());

        StageWithKeyAndWindow<PageVisit, Integer> windowStage = pageVisits.window(sliding(10, 1));

        windowStage.aggregate3(counting(), addToCarts, counting(), payments, counting())
                .writeTo(Sinks.logger());
        return p;
    }

    private static Pipeline coGroupWithBuilder() {
        Pipeline p = Pipeline.create();

        StreamStageWithKey<PageVisit, Integer> pageVisits = p
                .readFrom(Sources.<PageVisit, Integer, PageVisit>mapJournal(PAGE_VISIT,
                        START_FROM_OLDEST, mapEventNewValue(), mapPutEvents()))
                .withTimestamps(pv -> pv.timestamp(), 100)
                .groupingKey(pv -> pv.userId());
        StreamStageWithKey<AddToCart, Integer> addToCarts = p
                .readFrom(Sources.<AddToCart, Integer, AddToCart>mapJournal(ADD_TO_CART,
                        START_FROM_OLDEST, mapEventNewValue(), mapPutEvents()))
                .withTimestamps(atc -> atc.timestamp(), 100)
                .groupingKey(atc -> atc.userId());
        StreamStageWithKey<Payment, Integer> payments = p
                .readFrom(Sources.<Payment, Integer, Payment>mapJournal(PAYMENT,
                        START_FROM_OLDEST, mapEventNewValue(), mapPutEvents()))
                .withTimestamps(pm -> pm.timestamp(), 100)
                .groupingKey(pm -> pm.userId());

        StageWithKeyAndWindow<PageVisit, Integer> windowStage = pageVisits.window(sliding(10, 1));

        WindowGroupAggregateBuilder<Integer, Long> builder = windowStage.aggregateBuilder(counting());
        Tag<Long> pageVisitTag = builder.tag0();
        Tag<Long> addToCartTag = builder.add(addToCarts, counting());
        Tag<Long> paymentTag = builder.add(payments, counting());

        builder.build()
                .writeTo(Sinks.logger(r -> {
                    ItemsByTag items = r.result();
                    return String.format(
                            "window(%s..%s): id %d%n" +
                            "pageVisits %s%n" +
                            "addToCarts %s%n" +
                            "payments %s",
                            Util.toLocalTime(r.start()),
                            Util.toLocalTime(r.end()),
                            r.getKey(),
                            items.get(pageVisitTag),
                            items.get(addToCartTag),
                            items.get(paymentTag));
                }));
        return p;
    }

    private static class ProducerTask implements Runnable {
        private final IMap<Object, PageVisit> pageVisit;
        private final IMap<Object, AddToCart> addToCart;
        private final IMap<Object, Payment> payment;

        private volatile boolean keepGoing = true;

        private int loadTime = 1;
        private int quantity = 21;
        private int amount = 31;
        private long now = System.currentTimeMillis();

        ProducerTask(JetInstance jet) {
            pageVisit = jet.getMap(PAGE_VISIT);
            addToCart = jet.getMap(ADD_TO_CART);
            payment = jet.getMap(PAYMENT);

            new Thread(this, "WindowedCoGroup Producer").start();
        }

        @Override
        public void run() {
            LockSupport.parkNanos(MILLISECONDS.toNanos(100));
            while (keepGoing) {
                produceSampleData();
                LockSupport.parkNanos(MILLISECONDS.toNanos(1));
                now++;
            }
        }

        public void stop() {
            keepGoing = false;
        }

        private void produceSampleData() {
            for (int userId = 11; userId < 13; userId++) {
                for (int i = 0; i < 2; i++) {
                    pageVisit.set(TOPIC, new PageVisit(now, userId, loadTime));
                    addToCart.set(TOPIC, new AddToCart(now, userId, quantity));
                    payment.set(TOPIC, new Payment(now, userId, amount));

                    loadTime++;
                    quantity++;
                    amount++;
                }
            }
        }
    }
}

