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

import com.hazelcast.collection.IList;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.examples.cogroup.datamodel.AddToCart;
import com.hazelcast.jet.examples.cogroup.datamodel.PageVisit;
import com.hazelcast.jet.examples.cogroup.datamodel.Payment;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.BatchStageWithKey;
import com.hazelcast.jet.pipeline.GroupAggregateBuilder;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.IMap;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.toList;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;

/**
 * Demonstrates the usage of Pipeline API's co-group transformation, which
 * joins two or more streams on a common key and performs a user-specified
 * aggregate operation on the co-grouped items.
 */
@SuppressWarnings("Convert2MethodRef") // https://bugs.openjdk.java.net/browse/JDK-8154236
public final class BatchCoGroup {
    private static final String PAGE_VISIT = "pageVisit";
    private static final String ADD_TO_CART = "addToCart";
    private static final String PAYMENT = "payment";
    private static final String RESULT = "result";
    private final JetInstance jet;

    private final Map<Integer, Set<PageVisit>> userId2PageVisit = new HashMap<>();
    private final Map<Integer, Set<AddToCart>> userId2AddToCart = new HashMap<>();
    private final Map<Integer, Set<Payment>> userId2Payment = new HashMap<>();

    private BatchCoGroup(JetInstance jet) {
        this.jet = jet;
    }

    private static Pipeline coGroupDirect() {
        Pipeline p = Pipeline.create();

        // Create three source streams
        BatchStageWithKey<PageVisit, Integer> pageVisits =
                p.readFrom(Sources.<PageVisit>list(PAGE_VISIT))
                 .groupingKey(pageVisit -> pageVisit.userId());
        BatchStageWithKey<AddToCart, Integer> addToCarts =
                p.readFrom(Sources.<AddToCart>list(ADD_TO_CART))
                 .groupingKey(addToCart -> addToCart.userId());
        BatchStageWithKey<Payment, Integer> payments =
                p.readFrom(Sources.<Payment>list(PAYMENT))
                 .groupingKey(payment -> payment.userId());

        // Construct the co-group transform. The aggregate operation collects all
        // the stream items into a 3-tuple of lists.
        BatchStage<Entry<Integer, Tuple3<List<PageVisit>, List<AddToCart>, List<Payment>>>> coGrouped =
                pageVisits.aggregate3(toList(), addToCarts, toList(), payments, toList());

        // Store the results in the output map
        coGrouped.writeTo(Sinks.map(RESULT));
        return p;
    }

    private static Pipeline coGroupBuild() {
        Pipeline p = Pipeline.create();

        // Create three source streams
        BatchStageWithKey<PageVisit, Integer> pageVisits =
                p.readFrom(Sources.<PageVisit>list(PAGE_VISIT))
                 .groupingKey(pageVisit -> pageVisit.userId());
        BatchStageWithKey<AddToCart, Integer> addToCarts =
                p.readFrom(Sources.<AddToCart>list(ADD_TO_CART))
                 .groupingKey(addToCart -> addToCart.userId());
        BatchStageWithKey<Payment, Integer> payments =
                p.readFrom(Sources.<Payment>list(PAYMENT))
                 .groupingKey(payment -> payment.userId());

        // Obtain a builder object for the co-group transform
        GroupAggregateBuilder<Integer, List<PageVisit>> builder = pageVisits.aggregateBuilder(toList());
        Tag<List<PageVisit>> visitTag = builder.tag0();

        // Add the co-grouped streams to the builder. Here we add just two, but
        // you could add any number of them.
        Tag<List<AddToCart>> cartTag = builder.add(addToCarts, toList());
        Tag<List<Payment>> payTag = builder.add(payments, toList());

        // Build the co-group transform. The aggregate operation collects all the
        // stream items into ItemsByTag. We transform it into a 3-tuple of lists.
        BatchStage<Entry<Integer, Tuple3<List<PageVisit>, List<AddToCart>, List<Payment>>>> coGrouped = builder
                .build()
                .map(keyAndVals -> {
                    ItemsByTag ibt = keyAndVals.getValue();
                    return entry(keyAndVals.getKey(), tuple3(ibt.get(visitTag), ibt.get(cartTag), ibt.get(payTag)));
                });

        // Store the results in the output map
        coGrouped.writeTo(Sinks.map(RESULT));

        return p;
    }

    public static void main(String[] args) {
        JetInstance jet = Jet.bootstrappedInstance();
        new BatchCoGroup(jet).go();
    }

    private void go() {
        prepareSampleData();
        try {
            jet.newJob(coGroupDirect()).join();
            validateCoGroupResults();

            jet.getMap(RESULT).clear();

            jet.newJob(coGroupBuild()).join();
            validateCoGroupResults();
        } finally {
            Jet.shutdownAll();
        }
    }

    private void validateCoGroupResults() {
        IMap<Integer, Tuple3<List<PageVisit>, List<AddToCart>, List<Payment>>> result = jet.getMap(RESULT);
        printImap(result);
        for (int userId = 11; userId < 13; userId++) {
            Tuple3<List<PageVisit>, List<AddToCart>, List<Payment>> r = result.get(userId);
            assertEqual(userId2PageVisit.get(userId), r.f0());
            assertEqual(userId2AddToCart.get(userId), r.f1());
            assertEqual(userId2Payment.get(userId), r.f2());
        }
        System.out.println("BatchCoGroup results are valid");
    }

    private void prepareSampleData() {
        IList<AddToCart> addToCartList = jet.getList(ADD_TO_CART);
        IList<Payment> paymentList = jet.getList(PAYMENT);
        IList<PageVisit> pageVisitList = jet.getList(PAGE_VISIT);

        int quantity = 21;
        int amount = 31;
        int loadTime = 1;
        long timestamp = System.currentTimeMillis();
        for (int userId = 11; userId < 13; userId++) {
            userId2AddToCart.put(userId, new HashSet<>());
            userId2Payment.put(userId, new HashSet<>());
            userId2PageVisit.put(userId, new HashSet<>());
            for (int i = 0; i < 2; i++) {
                PageVisit visit = new PageVisit(timestamp, userId, loadTime);
                AddToCart atc = new AddToCart(timestamp, userId, quantity);
                Payment pay = new Payment(timestamp, userId, amount);

                addToCartList.add(atc);
                paymentList.add(pay);
                pageVisitList.add(visit);

                userId2AddToCart.get(userId).add(atc);
                userId2Payment.get(userId).add(pay);
                userId2PageVisit.get(userId).add(visit);

                loadTime++;
                quantity++;
                amount++;
                timestamp += 1000;
            }
        }
        printIList(addToCartList);
        printIList(paymentList);
        printIList(pageVisitList);
    }

    private static <T> void assertEqual(Set<T> expected, Collection<T> actual) {
        if (actual.size() != expected.size() || !expected.containsAll(actual)) {
            throw new AssertionError("Mismatch: expected " + expected + "; actual " + actual);
        }
    }

    private static <K, V> void printImap(IMap<K, V> imap) {
        StringBuilder sb = new StringBuilder();
        System.out.println(imap.getName() + ':');
        imap.forEach((k, v) -> sb.append(k).append("->").append(v).append('\n'));
        System.out.println(sb);
    }

    private static void printIList(IList<?> list) {
        StringBuilder sb = new StringBuilder();
        System.out.println(list.getName() + ':');
        list.forEach(e -> sb.append(e).append('\n'));
        System.out.println(sb);
    }
}
