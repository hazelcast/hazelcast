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

package com.hazelcast.jet.examples.enrichment;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.examples.enrichment.datamodel.Broker;
import com.hazelcast.jet.examples.enrichment.datamodel.Product;
import com.hazelcast.jet.examples.enrichment.datamodel.Trade;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.GeneralStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.map.IMap;
import com.hazelcast.replicatedmap.ReplicatedMap;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.stream.Stream;

import static com.hazelcast.function.Functions.entryValue;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static com.hazelcast.jet.pipeline.JoinClause.joinMapEntries;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_CURRENT;

/**
 * Demonstrates the usage of the Pipeline API to enrich a data stream. We
 * generate a stream of stock trade events and each event has an associated
 * product ID and broker ID. The reference lists of products and brokers
 * are stored in files. The goal is to enrich the trades with the actual
 * name of the products and the brokers.
 * <p>
 * This example shows different ways of achieving this goal:
 * <ol>
 *     <li>Using Hazelcast {@code IMap}</li>
 *     <li>Using Hazelcast {@code ReplicatedMap}</li>
 *     <li>Using an external service (gRPC in this sample)</li>
 *     <li>Using the pipeline {@code hashJoin} operation</li>
 * </ol>
 * <p>
 * The details of each approach are documented with the associated method.
 * <p>
 * We generate the stream of trade events by updating a single key in the
 * {@code trades} map which has the Event Journal enabled. The event
 * journal emits a stream of update events.
 */
public final class Enrichment {

    private static final String TRADES = "trades";
    private static final String PRODUCTS = "products";
    private static final String BROKERS = "brokers";

    private final JetInstance jet;


    private Enrichment(JetInstance jet) {
        this.jet = jet;
    }

    /**
     * Builds a pipeline which enriches the stream using an {@link IMap}.
     * <p>
     * It loads two {@code IMap}s with the data from the files and then looks
     * up from them for every incoming trade using the {@link
     * StreamStage#mapUsingIMap mapUsingIMap} transform. Since the
     * {@code IMap} is a distributed data structure, some of the lookups will
     * have to go through the network to another cluster member.
     * <p>
     * With this approach you can modify the data in the {@code IMap} while the
     * job is running and it will immediately see the changed data.
     */
    private Pipeline enrichUsingIMap() {
        IMap<Integer, Product> productMap = jet.getMap(PRODUCTS);
        readLines("products.txt").forEach(e -> productMap.put(e.getKey(), new Product(e.getKey(), e.getValue())));
        System.out.println("Loaded product map:");
        printMap(productMap);

        IMap<Integer, Broker> brokerMap = jet.getMap(BROKERS);
        readLines("brokers.txt").forEach(e -> brokerMap.put(e.getKey(), new Broker(e.getKey(), e.getValue())));
        System.out.println("Loaded brokers map:");
        printMap(brokerMap);

        Pipeline p = Pipeline.create();

        // The stream to be enriched: trades
        StreamStage<Trade> trades = p
                .readFrom(Sources.<Object, Trade>mapJournal(TRADES, START_FROM_CURRENT))
                .withoutTimestamps()
                .map(entryValue());

        // first enrich the trade by looking up the product from the IMap
        trades
                .mapUsingIMap(
                        productMap, // target map to lookup
                        trade -> trade.productId(), // key to lookup in the map
                        (t, product) -> tuple2(t, product.name()) // merge the value in the map with the trade
                )
                // (trade, productName)
                .mapUsingIMap(
                        brokerMap,
                        t -> t.f0().brokerId(),
                        (t, broker) -> tuple3(t.f0(), t.f1(), broker.name())
                )
                // (trade, productName, brokerName)
                .writeTo(Sinks.logger());

        return p;
    }

    /**
     * Builds a pipeline which enriches the stream using a {@link ReplicatedMap}.
     * <p>
     * It loads two {@code ReplicatedMap}s with the data from the files and
     * then looks up from them for every incoming trade using the {@link
     * StreamStage#mapUsingReplicatedMap mapUsingReplicatedMap} transform.
     * Since the {@code ReplicatedMap} replicates its complete contents on each
     * member, all the lookups will be local. Compared to the {@code IMap} this
     * means better performance, but also a higher memory cost.
     * <p>
     * With this approach you can modify the data in the {@code ReplicatedMap}
     * while the job is running and it will immediately see the changed data.
     */
    private Pipeline enrichUsingReplicatedMap() {
        ReplicatedMap<Integer, Product> productMap = jet.getReplicatedMap(PRODUCTS);
        readLines("products.txt").forEach(e -> productMap.put(e.getKey(), new Product(e.getKey(), e.getValue())));
        System.out.println("Loaded product replicated map:");
        printMap(productMap);

        ReplicatedMap<Integer, Broker> brokerMap = jet.getReplicatedMap(BROKERS);
        readLines("brokers.txt").forEach(e -> brokerMap.put(e.getKey(), new Broker(e.getKey(), e.getValue())));
        System.out.println("Loaded brokers replicated map:");
        printMap(brokerMap);

        Pipeline p = Pipeline.create();

        // The stream to be enriched: trades
        StreamStage<Trade> trades = p
                .readFrom(Sources.<Object, Trade>mapJournal(TRADES, START_FROM_CURRENT))
                .withoutTimestamps()
                .map(entryValue());

        // first enrich the trade by looking up the product from the replicated map
        trades
                .mapUsingReplicatedMap(
                        productMap, // target map to lookup
                        Trade::productId, // key to lookup in the map
                        (t, product) -> tuple2(t, product.name()) // merge the value in the map with the trade
                )
                // (trade, productName)
                .mapUsingReplicatedMap(
                        brokerMap,
                        t -> t.f0().brokerId(),
                        (t, broker) -> tuple3(t.f0(), t.f1(), broker.name())
                )
                // (trade, productName, brokerName)
                .writeTo(Sinks.logger());
        return p;
    }

    /**
     * Builds a pipeline which enriches the stream using the
     * {@linkplain GeneralStage#hashJoin hash-join} transform.
     * <p>
     * When using the hash join, you don't have to pre-load any maps with the
     * data. The Jet job will pull the data itself from files and store them
     * in internal hashtables. The hashtables are read-only so you can't keep
     * the data up-to-date while the job is running.
     * <p>
     * Like the {@code ReplicatedMap}, the hash-join transform stores all the
     * enriching data at all cluster members. The data is read-only so there
     * are no synchronization overheads, making this the fastest approach to
     * data enrichment.
     * <p>
     * Since the enriching data is stored internally with the running job, once
     * it completes the data is automatically released so there are no memory
     * leak issues to deal with.
     */
    private static Pipeline enrichUsingHashJoin() {
        Pipeline p = Pipeline.create();

        // The stream to be enriched: trades
        StreamStage<Trade> trades = p.readFrom(Sources.<Object, Trade>mapJournal(TRADES, START_FROM_CURRENT))
                                     .withoutTimestamps()
                                     .map(entryValue());

        // The enriching streams: products and brokers
        String resourcesPath = getClasspathDirectory("/").toString();
        BatchSource<Map.Entry<Integer, Product>> products = Sources
                .filesBuilder(resourcesPath)
                .sharedFileSystem(true)
                .glob("products.txt")
                .build((file, line) -> {
                    Map.Entry<Integer, String> split = splitLine(line);
                    return entry(split.getKey(), new Product(split.getKey(), split.getValue()));
                });

        BatchSource<Map.Entry<Integer, Broker>> brokers = Sources
                .filesBuilder(resourcesPath)
                .sharedFileSystem(true)
                .glob("brokers.txt")
                .build((file, line) -> {
                    Map.Entry<Integer, String> split = splitLine(line);
                    return entry(split.getKey(), new Broker(split.getKey(), split.getValue()));
                });

        BatchStage<Map.Entry<Integer, Product>> prodEntries = p.readFrom(products);
        BatchStage<Map.Entry<Integer, Broker>> brokEntries = p.readFrom(brokers);

        // Join the trade stream with the product and broker streams
        trades.hashJoin2(
                prodEntries, joinMapEntries(Trade::productId),
                brokEntries, joinMapEntries(Trade::brokerId),
                Tuple3::tuple3
        ).writeTo(Sinks.logger());

        return p;
    }

    public static void main(String[] args) throws Exception {
        JetInstance jet = Jet.bootstrappedInstance();

        new Enrichment(jet).go();
    }

    private void go() throws Exception {
        EventGenerator eventGenerator = new EventGenerator(jet.getMap(TRADES));
        eventGenerator.start();
        try {
            // comment out the code to try the appropriate enrichment method
            Pipeline p = enrichUsingIMap();
//            Pipeline p = enrichUsingReplicatedMap();
//            Pipeline p = enrichUsingHashJoin();
            Job job = jet.newJob(p);
            eventGenerator.generateEventsForFiveSeconds();
            job.cancel();
            try {
                job.join();
            } catch (CancellationException ignored) {
            }
        } finally {
            eventGenerator.shutdown();
            Jet.shutdownAll();
        }
    }

    private static Stream<Map.Entry<Integer, String>> readLines(String file) {
        try {
            InputStream stream = Enrichment.class.getResourceAsStream("/" + file);
            BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
            return reader.lines().map(Enrichment::splitLine);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Map.Entry<Integer, String> splitLine(String e) {
        int commaPos = e.indexOf(',');
        return entry(Integer.valueOf(e.substring(0, commaPos)), e.substring(commaPos + 1));
    }

    private static <K, V> void printMap(Map<K, V> imap) {
        StringBuilder sb = new StringBuilder();
        imap.forEach((k, v) -> sb.append(k).append("->").append(v).append('\n'));
        System.out.println(sb);
    }

    private static Path getClasspathDirectory(String name) {
        try {
            return Paths.get(Enrichment.class.getResource(name).toURI());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
