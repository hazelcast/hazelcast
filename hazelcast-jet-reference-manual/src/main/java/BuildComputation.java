/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.function.ComparatorEx;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.BatchStageWithKey;
import com.hazelcast.jet.pipeline.ContextFactory;
import com.hazelcast.jet.pipeline.GroupAggregateBuilder;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamHashJoinBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;
import datamodel.AddToCart;
import datamodel.Broker;
import datamodel.Delivery;
import datamodel.Market;
import datamodel.PageVisit;
import datamodel.Payment;
import datamodel.Person;
import datamodel.Product;
import datamodel.StockInfo;
import datamodel.Trade;
import datamodel.Tweet;

import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Stream;

import static com.hazelcast.client.HazelcastClient.newHazelcastClient;
import static com.hazelcast.jet.Traversers.traverseArray;
import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.Util.mapEventNewValue;
import static com.hazelcast.jet.Util.mapPutEvents;
import static com.hazelcast.jet.Util.toCompletableFuture;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.aggregate.AggregateOperations.maxBy;
import static com.hazelcast.jet.aggregate.AggregateOperations.toList;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.function.Functions.wholeItem;
import static com.hazelcast.jet.pipeline.JoinClause.joinMapEntries;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_CURRENT;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

class BuildComputation {
    static void s1() {
        //tag::s1[]
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<String>list("input"))
         .map(String::toUpperCase)
         .drainTo(Sinks.list("result"));
        //end::s1[]
    }

    static void s2() {
        //tag::s2[]
        Pipeline p = Pipeline.create();
        StreamStage<Trade> trades = p.drawFrom(tradeStream())
                                     .withoutTimestamps();
        BatchStage<Entry<Integer, Product>> products =
                p.drawFrom(Sources.map("products"));
        StreamStage<Tuple2<Trade, Product>> joined = trades.hashJoin(
                products,
                joinMapEntries(Trade::productId),
                Tuple2::tuple2
        );
        //end::s2[]
    }

    static void s3() {
        //tag::s3[]
        Pipeline p = Pipeline.create();
        BatchStage<String> src = p.drawFrom(Sources.list("src"));
        src.map(String::toUpperCase)
           .drainTo(Sinks.list("uppercase"));
        src.map(String::toLowerCase)
           .drainTo(Sinks.list("lowercase"));
        //end::s3[]
    }

    static Pipeline s4() {
        //tag::s4[]
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.list("text"))
         .aggregate(counting())
         .drainTo(Sinks.list("result"));
        //end::s4[]
        return p;
    }

    static void s4ab() {
        //tag::s4b[]
        System.setProperty("hazelcast.logging.type", "none");
        //end::s4b[]
        Pipeline p = s4();
        //tag::s4a[]
        JetInstance jet = Jet.newJetInstance();
        jet.getList("text").addAll(asList("foo foo bar", "foo bar foo"));
        jet.newJob(p).join();
        jet.getList("result").forEach(System.out::println);
        Jet.shutdownAll();
        //end::s4a[]
    }

    static Pipeline s5() {
        //tag::s5[]
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<String>list("text"))
         .flatMap(line -> traverseArray(line.toLowerCase().split("\\W+")))
         .aggregate(counting())
         .drainTo(Sinks.list("result"));
        //end::s5[]
        return p;
    }

    static Pipeline s6() {
        //tag::s6[]
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<String>list("text"))
         .flatMap(line -> traverseArray(line.toLowerCase().split("\\W+")))
         .groupingKey(wholeItem())
         .aggregate(counting())
         .drainTo(Sinks.list("result"));
        //end::s6[]
        return p;
    }

    static void s7() {
        //tag::s7[]
        // <1>
        JetInstance jet = Jet.newJetInstance();
        IList<PageVisit> pageVisits = jet.getList("pageVisit");
        IList<AddToCart> addToCarts = jet.getList("addToCart");
        IList<Entry<Integer, Double>> results = jet.getList("result");

        // <2>
        pageVisits.add(new PageVisit(1));
        pageVisits.add(new PageVisit(1));
        addToCarts.add(new AddToCart(1));

        pageVisits.add(new PageVisit(2));
        addToCarts.add(new AddToCart(2));

        // <3>
        Pipeline p = Pipeline.create();
        BatchStageWithKey<PageVisit, Integer> pageVisit =
                p.drawFrom(Sources.list(pageVisits))
                 .groupingKey(pv -> pv.userId());
        BatchStageWithKey<AddToCart, Integer> addToCart =
                p.drawFrom(Sources.list(addToCarts))
                 .groupingKey(atc -> atc.userId());

        BatchStage<Entry<Integer, Tuple2<Long, Long>>> coAggregated = pageVisit
                .aggregate2(counting(),         // <4>
                        addToCart, counting()); // <5>
        coAggregated
                .map(e -> {                     // <6>
                    long visitCount = e.getValue().f0();
                    long addToCartCount = e.getValue().f1();
                    return entry(e.getKey(), (double) (addToCartCount / visitCount));
                })
                .drainTo(Sinks.list(results));
        // <7>
        jet.newJob(p).join();
        results.forEach(System.out::println);
        Jet.shutdownAll();
        //end::s7[]
    }

    static void s8() {
        Pipeline p = Pipeline.create();

        //tag::s8[]
        BatchStageWithKey<PageVisit, Integer> pageVisit =
                p.drawFrom(Sources.<PageVisit>list("pageVisit"))
                 .groupingKey(PageVisit::userId);
        BatchStageWithKey<AddToCart, Integer> addToCart =
                p.drawFrom(Sources.<AddToCart>list("addToCart"))
                 .groupingKey(AddToCart::userId);
        //end::s8[]

        //tag::s8a[]
        BatchStage<Tuple2<List<PageVisit>, List<AddToCart>>> joinedLists =
            pageVisit.aggregate2(toList(), addToCart, toList())
                     .map(Entry::getValue);
        //end::s8a[]

        //tag::s8b[]
        BatchStage<Tuple2<List<PageVisit>, List<AddToCart>>> leftOuterJoined =
                joinedLists.filter(pair -> !pair.f0().isEmpty());

        //end::s8b[]

        //tag::s8c[]
        BatchStage<Tuple2<PageVisit, AddToCart>> fullJoined = joinedLists
            .flatMap(pair -> traverseStream(
                    nonEmptyStream(pair.f0())
                        .flatMap(pVisit -> nonEmptyStream(pair.f1())
                                .map(addCart -> tuple2(pVisit, addCart)))));
        //end::s8c[]

        leftOuterJoined.drainTo(Sinks.map("result"));
    }

    //tag::nonEmptyStream[]
    static <T> Stream<T> nonEmptyStream(List<T> input) {
        return input.isEmpty() ? Stream.of((T) null) : input.stream();
    }
    //end::nonEmptyStream[]

    static void s9() {
        //tag::s9[]
        Pipeline p = Pipeline.create();

        //<1>
        BatchStageWithKey<PageVisit, Integer> pageVisits =
                p.drawFrom(Sources.<PageVisit>list("pageVisit"))
                 .groupingKey(PageVisit::userId);
        BatchStageWithKey<AddToCart, Integer> addToCarts =
                p.drawFrom(Sources.<AddToCart>list("addToCart"))
                 .groupingKey(AddToCart::userId);
        BatchStageWithKey<Payment, Integer> payments =
                p.drawFrom(Sources.<Payment>list("payment"))
                 .groupingKey(Payment::userId);
        BatchStageWithKey<Delivery, Integer> deliveries =
                p.drawFrom(Sources.<Delivery>list("delivery"))
                 .groupingKey(Delivery::userId);

        //<2>
        GroupAggregateBuilder<Integer, List<PageVisit>> builder =
                pageVisits.aggregateBuilder(toList());

        //<3>
        Tag<List<PageVisit>> visitTag = builder.tag0();
        Tag<List<AddToCart>> cartTag = builder.add(addToCarts, toList());
        Tag<List<Payment>> payTag = builder.add(payments, toList());
        Tag<List<Delivery>> deliveryTag = builder.add(deliveries, toList());

        //<4>
        BatchStage<String> coGrouped = builder
                .build()
                .map(e -> {
                    ItemsByTag ibt = e.getValue();
                    return String.format("User ID %d: %d visits, %d add-to-carts," +
                                    " %d payments, %d deliveries",
                            e.getKey(), ibt.get(visitTag).size(), ibt.get(cartTag).size(),
                            ibt.get(payTag).size(), ibt.get(deliveryTag).size());
                });
        //end::s9[]
    }

    static void s10() {
        JetInstance instance = Jet.newJetInstance();
        //tag::s10[]
        Pipeline p = Pipeline.create();

        // The primary stream (stream to be enriched): trades
        IMap<Long, Trade> tradesMap = instance.getMap("trades");
        StreamStage<Trade> trades = p.drawFrom(
                Sources.mapJournal(tradesMap, mapPutEvents(),
                        mapEventNewValue(), START_FROM_CURRENT))
                .withoutTimestamps();

        // The enriching streams: products and brokers
        BatchStage<Entry<Integer, Product>> prodEntries =
                p.drawFrom(Sources.map("products"));
        BatchStage<Entry<Integer, Broker>> brokEntries =
                p.drawFrom(Sources.map("brokers"));

        // Join the trade stream with the product and broker streams
        StreamStage<Tuple3<Trade, Product, Broker>> joined = trades.hashJoin2(
                prodEntries, joinMapEntries(Trade::productId),
                brokEntries, joinMapEntries(Trade::brokerId),
                Tuple3::tuple3
        );
        //end::s10[]
    }

    static void s10a() {
        Pipeline p = Pipeline.create();
        StreamStage<Trade> trades = null;
        BatchStage<Entry<Integer, Product>> prodEntries =
                p.drawFrom(Sources.map("products"));

        //tag::s10a[]
        StreamStage<Trade> joined = trades.hashJoin(
                prodEntries,
                joinMapEntries(Trade::productId),
                Trade::setProduct // <1>
        );
        //end::s10a[]
    }



    static void s11() {
        JetInstance instance = Jet.newJetInstance();
        //tag::s11[]
        Pipeline p = Pipeline.create();

        // The stream to be enriched: trades
        IMap<Long, Trade> tradesMap = instance.getMap("trades");
        StreamStage<Trade> trades = p.drawFrom(
                Sources.mapJournal(tradesMap, mapPutEvents(),
                        mapEventNewValue(), START_FROM_CURRENT))
                .withoutTimestamps();

        // The enriching streams: products, brokers and markets
        BatchStage<Entry<Integer, Product>> prodEntries =
                p.drawFrom(Sources.map("products"));
        BatchStage<Entry<Integer, Broker>> brokEntries =
                p.drawFrom(Sources.map("brokers"));
        BatchStage<Entry<Integer, Market>> marketEntries =
                p.drawFrom(Sources.map("markets"));

        // Obtain a hash-join builder object from the stream to be enriched
        StreamHashJoinBuilder<Trade> builder = trades.hashJoinBuilder();

        // Add enriching streams to the builder
        Tag<Product> productTag = builder.add(prodEntries,
                joinMapEntries(Trade::productId));
        Tag<Broker> brokerTag = builder.add(brokEntries,
                joinMapEntries(Trade::brokerId));
        Tag<Market> marketTag = builder.add(marketEntries,
                joinMapEntries(Trade::marketId));

        // Build the hash join pipeline
        StreamStage<Tuple2<Trade, ItemsByTag>> joined = builder.build(Tuple2::tuple2);
        //end::s11[]

        //tag::s12[]
        StreamStage<String> mapped = joined.map(
                (Tuple2<Trade, ItemsByTag> tuple) -> {
                    Trade trade = tuple.f0();
                    ItemsByTag ibt = tuple.f1();
                    Product product = ibt.get(productTag);
                    Broker broker = ibt.get(brokerTag);
                    Market market = ibt.get(marketTag);
                    return trade + ": " + product + ", " + broker + ", " + market;
                });
        //end::s12[]
    }

    static void s13a() {
        Pipeline p = Pipeline.create();
        //tag::s13a[]
        BatchStage<Tweet> tweets = p.drawFrom(Sources.list("tweets"));

        tweets.flatMap(tweet -> traverseArray(tweet.text().toLowerCase().split("\\W+")))
              .filter(word -> !word.isEmpty())
              .groupingKey(wholeItem())
              .aggregate(counting())
              .drainTo(Sinks.map("counts"));
        //end::s13a[]
    }

    static void s13() {
        Pipeline p = Pipeline.create();
        //tag::s13[]
        StreamStage<Tweet> tweets = p.drawFrom(twitterStream())
                                     .withNativeTimestamps(0); // <1>

        tweets.flatMap(tweet -> traverseArray(tweet.text().toLowerCase().split("\\W+")))
              .filter(word -> !word.isEmpty())
              .window(sliding(MINUTES.toMillis(1), SECONDS.toMillis(1))) // <2>
              .groupingKey(wholeItem())
              .aggregate(counting())
              .drainTo(Sinks.list("result"));
        //end::s13[]
    }

    static void s14() {
        Pipeline p = Pipeline.create();
        //tag::s14[]
        StreamStage<Tweet> tweets = p.drawFrom(twitterStream())
                         .withTimestamps(Tweet::timestamp, SECONDS.toMillis(5));
        //end::s14[]
    }

    static void s15() {
        Pipeline p = Pipeline.create();

        //tag::s15[]
        StreamStage<KeyedWindowResult<String, Long>> result =
            p.drawFrom(twitterStream())
             .withTimestamps(Tweet::timestamp, SECONDS.toMillis(15))
             .flatMap(tweet -> traverseArray(tweet.text().toLowerCase().split("\\W+")))
             .filter(word -> !word.isEmpty())
             .window(sliding(MINUTES.toMillis(1), SECONDS.toMillis(1))
                     .setEarlyResultsPeriod(SECONDS.toMillis(1)))
             .groupingKey(wholeItem())
             .aggregate(counting());
        //end::s15[]
    }

    static void s16() {
        JetInstance jet = Jet.newJetInstance();
        //tag::s16[]
        IMap<String, StockInfo> stockMap = jet.getMap("stock-info"); //<1>
        StreamSource<Trade> tradesSource = Sources.mapJournal("trades",
                mapPutEvents(), mapEventNewValue(), START_FROM_CURRENT);

        Pipeline p = Pipeline.create();
        p.drawFrom(tradesSource)
         .withoutTimestamps()
         .groupingKey(Trade::ticker) // <2>
         .mapUsingIMap(stockMap, Trade::setStockInfo) //<3>
         .drainTo(Sinks.list("result"));
        //end::s16[]
    }

    static void s16a() {
        //tag::s16a[]
        ContextFactory<IMap<String, StockInfo>> ctxFac = ContextFactory
                .withCreateFn(x -> {
                    ClientConfig cc = new ClientConfig();
                    cc.getNearCacheConfigMap().put("stock-info",
                            new NearCacheConfig());
                    HazelcastInstance client = newHazelcastClient(cc);
                    IMap<String, StockInfo> map = client.getMap("stock-info");
                    return map;
                })
                .withLocalSharing();
        StreamSource<Trade> tradesSource = Sources.mapJournal("trades",
                mapPutEvents(), mapEventNewValue(), START_FROM_CURRENT);

        Pipeline p = Pipeline.create();
        p.drawFrom(tradesSource)
         .withoutTimestamps()
         .groupingKey(Trade::ticker)
         .mapUsingContextAsync(ctxFac,
                 (map, key, trade) -> toCompletableFuture(map.getAsync(key))
                         .thenApply(trade::setStockInfo))
         .drainTo(Sinks.list("result"));
        //end::s16a[]
    }

    static void s17() {
        //tag::s17[]
        Pipeline p = Pipeline.create();
        BatchSource<Person> personSource = Sources.list("people");
        p.drawFrom(personSource)
         .groupingKey(person -> person.getAge() / 5)
         .distinct()
         .drainTo(Sinks.list("sampleByAgeBracket"));
        //end::s17[]
    }

    static void s18() {
        JetInstance instance = Jet.newJetInstance();
        //tag::s18[]
        Pipeline p = Pipeline.create();
        IMap<Long, Trade> tradesNewYorkMap = instance.getMap("trades-newyork");
        IMap<Long, Trade> tradesTokyoMap = instance.getMap("trades-tokyo");
        StreamStage<Trade> tradesNewYork = p.drawFrom(
                Sources.mapJournal(tradesNewYorkMap, mapPutEvents(),
                        mapEventNewValue(), START_FROM_CURRENT))
                .withNativeTimestamps(5_000);
        StreamStage<Trade> tradesTokyo = p.drawFrom(
                Sources.mapJournal(tradesTokyoMap, mapPutEvents(),
                        mapEventNewValue(), START_FROM_CURRENT))
                .withNativeTimestamps(5_000);
        StreamStage<Trade> merged = tradesNewYork.merge(tradesTokyo);
        //end::s18[]
    }

    static void s19() {
        //tag::s19[]
        Pipeline p = Pipeline.create();
        StreamSource<Trade> tradesSource = Sources.mapJournal("trades",
                mapPutEvents(), mapEventNewValue(), START_FROM_CURRENT);
        StreamStage<Trade> currLargestTrade =
                p.drawFrom(tradesSource)
                 .withoutTimestamps()
                 .rollingAggregate(maxBy(
                         ComparatorEx.comparing(Trade::worth)));
        //end::s19[]
    }

    private static StreamSource<Trade> tradeStream() {
        return null;
    }

    private static StreamSource<Tweet> twitterStream() {
        return null;
    }


    static void apply() {
        Pipeline p = Pipeline.create();
        BatchSource<String> source = null;
        {
            //tag::apply2[]
            BatchStage<String> stage = p.drawFrom(source);
            BatchStage<String> cleanedUp = PipelineTransforms.cleanUp(stage);
            BatchStage<Long> counted = cleanedUp.aggregate(counting());
            //end::apply2[]
        }
        //tag::apply3[]
        BatchStage<Long> counted = p.drawFrom(source)
                       .apply(PipelineTransforms::cleanUp)
                       .aggregate(counting());
        //end::apply3[]

    }

    static class PipelineTransforms {
        //tag::apply1[]
        static BatchStage<String> cleanUp(BatchStage<String> input) {
            return input.map(String::toLowerCase)
                        .filter(s -> s.startsWith("success"));
        }
        //end::apply1[]
    }

    private static Traverser<String> fooFlatMap(String t) {
        return null;
    }

    private static String fooMap(String t) {
        return null;
    }
}

