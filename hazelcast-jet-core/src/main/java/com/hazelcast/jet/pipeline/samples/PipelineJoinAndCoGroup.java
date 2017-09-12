/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.pipeline.samples;

import com.hazelcast.core.IMap;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.pipeline.CoGroupBuilder;
import com.hazelcast.jet.pipeline.ComputeStage;
import com.hazelcast.jet.pipeline.HashJoinBuilder;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.datamodel.BagsByTag;
import com.hazelcast.jet.pipeline.datamodel.ItemsByTag;
import com.hazelcast.jet.pipeline.datamodel.Tag;
import com.hazelcast.jet.pipeline.datamodel.ThreeBags;
import com.hazelcast.jet.pipeline.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.datamodel.Tuple3;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.pipeline.JoinClause.joinMapEntries;

public class PipelineJoinAndCoGroup {

    private static final String TRADES = "trades";
    private static final String PRODUCTS = "products";
    private static final String BROKERS = "brokers";
    private static final String RESULT = "result";
    private static final String RESULT_BROKER = "result_broker";

    private final JetInstance jet;
    private Pipeline p = Pipeline.create();
    private final ComputeStage<Entry<Integer, Trade>> tradEntries =
            p.drawFrom(Sources.<Integer, Trade>readMap(TRADES));
    private final ComputeStage<Trade> trades = tradEntries.map(Entry::getValue);
    private final ComputeStage<Entry<Integer, Product>> prodEntries =
            p.drawFrom(Sources.<Integer, Product>readMap(PRODUCTS));
    private final ComputeStage<Entry<Integer, Broker>> brokEntries =
            p.drawFrom(Sources.<Integer, Broker>readMap(BROKERS));
    private final ComputeStage<Product> products = prodEntries.map(Entry::getValue);
    private final ComputeStage<Broker> brokers = brokEntries.map(Entry::getValue);

    private Tag<Trade> tradeTag;
    private Tag<Product> productTag;
    private Tag<Broker> brokerTag;

    private final Map<Integer, Set<Trade>> class2trade = new HashMap<>();
    private final Map<Integer, Set<Product>> class2product = new HashMap<>();
    private final Map<Integer, Set<Broker>> class2broker = new HashMap<>();

    private PipelineJoinAndCoGroup(JetInstance jet) {
        this.jet = jet;
    }

    public static void main(String[] args) throws Exception {
        JetInstance jet = Jet.newJetInstance();
        Jet.newJetInstance();
        PipelineJoinAndCoGroup sample = new PipelineJoinAndCoGroup(jet);
        try {
            sample.prepareSampleData();
            sample.coGroupBuild().drainTo(Sinks.writeMap(RESULT));
            // This line added to test multiple outputs from a Stage
            sample.tradEntries.drainTo(Sinks.writeMap(RESULT_BROKER));
            sample.execute();
            printImap(jet.getMap(RESULT_BROKER));
            sample.validateCoGroupBuildResults();
        } finally {
            Jet.shutdownAll();
        }
    }

    private void prepareSampleData() {
        IMap<Integer, Product> productMap = jet.getMap(PRODUCTS);
        IMap<Integer, Broker> brokerMap = jet.getMap(BROKERS);
        IMap<Integer, Trade> tradeMap = jet.getMap(TRADES);

        int productId = 21;
        int brokerId = 31;
        int tradeId = 1;
        for (int classId = 11; classId < 13; classId++) {
            class2product.put(classId, new HashSet<>());
            class2broker.put(classId, new HashSet<>());
            class2trade.put(classId, new HashSet<>());
            for (int i = 0; i < 2; i++) {
                Product prod = new Product(classId, productId);
                Broker brok = new Broker(classId, brokerId);
                Trade trad = new Trade(tradeId, classId, productId, brokerId);

                productMap.put(productId, prod);
                brokerMap.put(brokerId, brok);
                tradeMap.put(tradeId, trad);

                class2product.get(classId).add(prod);
                class2broker.get(classId).add(brok);
                class2trade.get(classId).add(trad);

                tradeId++;
                productId++;
                brokerId++;
            }
        }
        printImap(productMap);
        printImap(brokerMap);
        printImap(tradeMap);
    }

    private ComputeStage<Entry<Integer, Tuple3<Trade, Product, Broker>>> joinDirect() {
        ComputeStage<Tuple3<Trade, Product, Broker>> joined = trades.hashJoin(
                prodEntries, joinMapEntries(Trade::productId),
                brokEntries, joinMapEntries(Trade::brokerId)
        );
        return joined.map(t -> entry(t.f0().id(), t));
    }

    private void validateJoinDirectResults() {
        IMap<Integer, Tuple3<Trade, Product, Broker>> result = jet.getMap(RESULT);
        printImap(result);
        for (int tradeId = 1; tradeId < 5; tradeId++) {
            Tuple3<Trade, Product, Broker> value = result.get(tradeId);
            Trade trade = value.f0();
            Product product = value.f1();
            Broker broker = value.f2();
            assertEquals(trade.productId(), product.id());
            assertEquals(trade.brokerId(), broker.id());
        }
        System.err.println("JoinDirect results are valid");
    }

    private ComputeStage<Entry<Integer, Tuple2<Trade, ItemsByTag>>> joinBuild() {
        HashJoinBuilder<Trade> builder = trades.hashJoinBuilder();
        productTag = builder.add(prodEntries, joinMapEntries(Trade::productId));
        brokerTag = builder.add(brokEntries, joinMapEntries(Trade::brokerId));
        ComputeStage<Tuple2<Trade, ItemsByTag>> joined = builder.build();
        return joined.map(t -> entry(t.f0().id(), t));
    }

    private void validateJoinBuildResults() {
        IMap<Integer, Tuple2<Trade, ItemsByTag>> result = jet.getMap(RESULT);
        printImap(result);
        for (int tradeId = 1; tradeId < 5; tradeId++) {
            Tuple2<Trade, ItemsByTag> value = result.get(tradeId);
            Trade trade = value.f0();
            ItemsByTag map = value.f1();
            Product product = map.get(productTag);
            Broker broker = map.get(brokerTag);
            assertEquals(trade.productId(), product.id());
            assertEquals(trade.brokerId(), broker.id());
        }
        System.err.println("JoinBuild results are valid");
    }

    private ComputeStage<Tuple2<Integer, ThreeBags>> coGroupDirect() {
        return trades.coGroup(
                Trade::classId,
                products, Product::classId,
                brokers, Broker::classId,
                AggregateOperation
                        .withCreate(ThreeBags<Trade, Product, Broker>::new)
                        .<Trade>andAccumulate0((acc, trade) -> acc.bag0().add(trade))
                        .<Product>andAccumulate1((acc, product) -> acc.bag1().add(product))
                        .<Broker>andAccumulate2((acc, broker) -> acc.bag2().add(broker))
                        .andCombine(ThreeBags::combineWith)
                        .andFinish(x -> x));
    }

    private void validateCoGroupDirectResults() {
        IMap<Integer, ThreeBags<Trade, Product, Broker>> result = jet.getMap(RESULT);
        printImap(result);
        for (int classId = 11; classId < 13; classId++) {
            ThreeBags<Trade, Product, Broker> bags = result.get(classId);
            assertEqual(class2trade.get(classId), bags.bag0());
            assertEqual(class2product.get(classId), bags.bag1());
            assertEqual(class2broker.get(classId), bags.bag2());
        }
        System.err.println("CoGroupDirect results are valid");
    }

    private ComputeStage<Tuple2<Integer, BagsByTag>> coGroupBuild() {
        CoGroupBuilder<Integer, Trade> builder = trades.coGroupBuilder(Trade::classId);
        Tag<Trade> tradeTag = this.tradeTag = builder.tag0();
        Tag<Product> productTag = this.productTag = builder.add(products, Product::classId);
        Tag<Broker> brokerTag = this.brokerTag = builder.add(brokers, Broker::classId);

        return builder.build(AggregateOperation
                .withCreate(BagsByTag::new)
                .andAccumulate(tradeTag, (acc, trade) -> acc.ensureBag(tradeTag).add(trade))
                .andAccumulate(productTag, (acc, product) -> acc.ensureBag(productTag).add(product))
                .andAccumulate(brokerTag, (acc, broker) -> acc.ensureBag(brokerTag).add(broker))
                .andCombine(BagsByTag::combineWith)
                .andFinish(x -> x)
        );
    }

    private void validateCoGroupBuildResults() {
        IMap<Integer, BagsByTag> result = jet.getMap(RESULT);
        printImap(result);
        for (int classId = 11; classId < 13; classId++) {
            BagsByTag bags = result.get(classId);
            assertEqual(class2trade.get(classId), bags.bag(tradeTag));
            assertEqual(class2product.get(classId), bags.bag(productTag));
            assertEqual(class2broker.get(classId), bags.bag(brokerTag));
        }
        System.err.println("CoGroupBuild results are valid");
    }

    private void execute() throws Exception {
        p.execute(jet).get();
    }

    static void assertEquals(long expected, long actual) {
        if (expected != actual) {
            throw new AssertionError("Expected != actual: " + expected + " != " + actual);
        }
    }

    private static <T> void assertEqual(Set<T> expected, Collection<T> actual) {
        if (actual.size() != expected.size() || !expected.containsAll(actual)) {
            throw new AssertionError("Mismatch: expected " + expected + "; actual " + actual);
        }
    }

    static <K, V> void printImap(IMap<K, V> imap) {
        StringBuilder sb = new StringBuilder();
        System.err.println(imap.getName() + ':');
        imap.forEach((k, v) -> sb.append(k).append("->").append(v).append('\n'));
        System.err.println(sb);
    }

}
