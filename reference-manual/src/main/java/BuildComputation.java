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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.collection.IList;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.ComparatorEx;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.examples.enrichment.datamodel.AddToCart;
import com.hazelcast.jet.examples.enrichment.datamodel.Broker;
import com.hazelcast.jet.examples.enrichment.datamodel.Delivery;
import com.hazelcast.jet.examples.enrichment.datamodel.Market;
import com.hazelcast.jet.examples.enrichment.datamodel.PageVisit;
import com.hazelcast.jet.examples.enrichment.datamodel.Payment;
import com.hazelcast.jet.examples.enrichment.datamodel.Person;
import com.hazelcast.jet.examples.enrichment.datamodel.Product;
import com.hazelcast.jet.examples.enrichment.datamodel.StockInfo;
import com.hazelcast.jet.examples.enrichment.datamodel.Trade;
import com.hazelcast.jet.examples.enrichment.datamodel.Tweet;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.BatchStageWithKey;
import com.hazelcast.jet.pipeline.GroupAggregateBuilder;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamHashJoinBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.map.IMap;

import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Stream;

import static com.hazelcast.client.HazelcastClient.newHazelcastClient;
import static com.hazelcast.function.Functions.wholeItem;
import static com.hazelcast.jet.Traversers.traverseArray;
import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.Util.mapEventNewValue;
import static com.hazelcast.jet.Util.mapPutEvents;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.aggregate.AggregateOperations.maxBy;
import static com.hazelcast.jet.aggregate.AggregateOperations.toList;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
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
        p.readFrom(Sources.<String>list("input"))
         .map(String::toUpperCase)
         .writeTo(Sinks.list("result"));
        //end::s1[]
    }

    static void s2() {
        //tag::s2[]
        Pipeline p = Pipeline.create();
        StreamStage<Trade> trades = p.readFrom(tradeStream())
                                     .withoutTimestamps();
        BatchStage<Entry<Integer, Product>> products =
                p.readFrom(Sources.map("products"));
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
        BatchStage<String> src = p.readFrom(Sources.list("src"));
        src.map(String::toUpperCase)
           .writeTo(Sinks.list("uppercase"));
        src.map(String::toLowerCase)
           .writeTo(Sinks.list("lowercase"));
        //end::s3[]
    }

    static Pipeline s4() {
        //tag::s4[]
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.list("text"))
         .aggregate(counting())
         .writeTo(Sinks.list("result"));
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
        p.readFrom(Sources.<String>list("text"))
         .flatMap(line -> traverseArray(line.toLowerCase().split("\\W+")))
         .aggregate(counting())
         .writeTo(Sinks.list("result"));
        //end::s5[]
        return p;
    }

    static Pipeline s6() {
        //tag::s6[]
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.<String>list("text"))
         .flatMap(line -> traverseArray(line.toLowerCase().split("\\W+")))
         .groupingKey(wholeItem())
         .aggregate(counting())
         .writeTo(Sinks.list("result"));
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
                p.readFrom(Sources.list(pageVisits))
                 .groupingKey(pv -> pv.userId());
        BatchStageWithKey<AddToCart, Integer> addToCart =
                p.readFrom(Sources.list(addToCarts))
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
                .writeTo(Sinks.list(results));
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
                p.readFrom(Sources.<PageVisit>list("pageVisit"))
                 .groupingKey(PageVisit::userId);
        BatchStageWithKey<AddToCart, Integer> addToCart =
                p.readFrom(Sources.<AddToCart>list("addToCart"))
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

        leftOuterJoined.writeTo(Sinks.map("result"));
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
                p.readFrom(Sources.<PageVisit>list("pageVisit"))
                 .groupingKey(PageVisit::userId);
        BatchStageWithKey<AddToCart, Integer> addToCarts =
                p.readFrom(Sources.<AddToCart>list("addToCart"))
                 .groupingKey(AddToCart::userId);
        BatchStageWithKey<Payment, Integer> payments =
                p.readFrom(Sources.<Payment>list("payment"))
                 .groupingKey(Payment::userId);
        BatchStageWithKey<Delivery, Integer> deliveries =
                p.readFrom(Sources.<Delivery>list("delivery"))
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
        StreamStage<Trade> trades = p.readFrom(
                Sources.mapJournal(tradesMap, START_FROM_CURRENT, mapEventNewValue(), mapPutEvents()
                ))
                .withoutTimestamps();

        // The enriching streams: products and brokers
        BatchStage<Entry<Integer, Product>> prodEntries =
                p.readFrom(Sources.map("products"));
        BatchStage<Entry<Integer, Broker>> brokEntries =
                p.readFrom(Sources.map("brokers"));

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
                p.readFrom(Sources.map("products"));

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
        StreamStage<Trade> trades = p.readFrom(
                Sources.mapJournal(tradesMap, START_FROM_CURRENT, mapEventNewValue(), mapPutEvents()
                ))
                .withoutTimestamps();

        // The enriching streams: products, brokers and markets
        BatchStage<Entry<Integer, Product>> prodEntries =
                p.readFrom(Sources.map("products"));
        BatchStage<Entry<Integer, Broker>> brokEntries =
                p.readFrom(Sources.map("brokers"));
        BatchStage<Entry<Integer, Market>> marketEntries =
                p.readFrom(Sources.map("markets"));

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
        BatchStage<Tweet> tweets = p.readFrom(Sources.list("tweets"));

        tweets.flatMap(tweet -> traverseArray(tweet.text().toLowerCase().split("\\W+")))
              .filter(word -> !word.isEmpty())
              .groupingKey(wholeItem())
              .aggregate(counting())
              .writeTo(Sinks.map("counts"));
        //end::s13a[]
    }

    static void s13() {
        Pipeline p = Pipeline.create();
        //tag::s13[]
        StreamStage<Tweet> tweets = p.readFrom(twitterStream())
                                     .withNativeTimestamps(0); // <1>

        tweets.flatMap(tweet -> traverseArray(tweet.text().toLowerCase().split("\\W+")))
              .filter(word -> !word.isEmpty())
              .window(sliding(MINUTES.toMillis(1), SECONDS.toMillis(1))) // <2>
              .groupingKey(wholeItem())
              .aggregate(counting())
              .writeTo(Sinks.list("result"));
        //end::s13[]
    }

    static void s14() {
        Pipeline p = Pipeline.create();
        //tag::s14[]
        StreamStage<Tweet> tweets = p.readFrom(twitterStream())
                         .withTimestamps(Tweet::timestamp, SECONDS.toMillis(5));
        //end::s14[]
    }

    static void s15() {
        Pipeline p = Pipeline.create();

        //tag::s15[]
        StreamStage<KeyedWindowResult<String, Long>> result =
            p.readFrom(twitterStream())
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
                START_FROM_CURRENT, mapEventNewValue(), mapPutEvents());

        Pipeline p = Pipeline.create();
        p.readFrom(tradesSource)
         .withoutTimestamps()
         .groupingKey(Trade::ticker) // <2>
         .mapUsingIMap(stockMap, Trade::setStockInfo) //<3>
         .writeTo(Sinks.list("result"));
        //end::s16[]
    }

    static void s16a() {
        //tag::s16a[]
        ServiceFactory<?, IMap<String, StockInfo>> svcFac = ServiceFactories.sharedService(
                pctx -> {
                    ClientConfig cc = new ClientConfig();
                    cc.getNearCacheConfigMap().put("stock-info",
                            new NearCacheConfig());
                    HazelcastInstance client = newHazelcastClient(cc);
                    IMap<String, StockInfo> map = client.getMap("stock-info");
                    return map;
                });
        StreamSource<Trade> tradesSource = Sources.mapJournal("trades",
                START_FROM_CURRENT, mapEventNewValue(), mapPutEvents());

        Pipeline p = Pipeline.create();
        p.readFrom(tradesSource)
         .withoutTimestamps()
         .groupingKey(Trade::ticker)
         .mapUsingServiceAsync(svcFac,
                 (map, key, trade) -> map.getAsync(key).toCompletableFuture()
                         .thenApply(trade::setStockInfo))
         .writeTo(Sinks.list("result"));
        //end::s16a[]
    }

    static void s17() {
        //tag::s17[]
        Pipeline p = Pipeline.create();
        BatchSource<Person> personSource = Sources.list("people");
        p.readFrom(personSource)
         .groupingKey(person -> person.getAge() / 5)
         .distinct()
         .writeTo(Sinks.list("sampleByAgeBracket"));
        //end::s17[]
    }

    static void s18() {
        JetInstance instance = Jet.newJetInstance();
        //tag::s18[]
        Pipeline p = Pipeline.create();
        IMap<Long, Trade> tradesNewYorkMap = instance.getMap("trades-newyork");
        IMap<Long, Trade> tradesTokyoMap = instance.getMap("trades-tokyo");
        StreamStage<Trade> tradesNewYork = p.readFrom(
                Sources.mapJournal(tradesNewYorkMap, START_FROM_CURRENT, mapEventNewValue(), mapPutEvents()
                ))
                .withNativeTimestamps(5_000);
        StreamStage<Trade> tradesTokyo = p.readFrom(
                Sources.mapJournal(tradesTokyoMap, START_FROM_CURRENT, mapEventNewValue(), mapPutEvents()
                ))
                .withNativeTimestamps(5_000);
        StreamStage<Trade> merged = tradesNewYork.merge(tradesTokyo);
        //end::s18[]
    }

    static void s19() {
        //tag::s19[]
        Pipeline p = Pipeline.create();
        StreamSource<Trade> tradesSource = Sources.mapJournal("trades",
                START_FROM_CURRENT, mapEventNewValue(), mapPutEvents());
        StreamStage<Trade> currLargestTrade =
                p.readFrom(tradesSource)
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

    private static final long TIMED_OUT = -1;
    private static final int TRANSACTION_START = 0;
    private static final int TRANSACTION_END = 1;

    static void s20() {
        Pipeline p = Pipeline.create();
        //tag::s20[]
        StreamStage<Entry<String, Long>> transactionOutcomes = eventStream() // <1>
            .groupingKey(TransactionEvent::transactionId)
            .mapStateful(
                SECONDS.toMillis(2),
                () -> new TransactionEvent[2], // <2>
                (startEnd, transactionId, transactionEvent) -> { // <3>
                    switch (transactionEvent.type()) {
                        case TRANSACTION_START:
                            startEnd[0] = transactionEvent;
                            break;
                        case TRANSACTION_END:
                            startEnd[1] = transactionEvent;
                            break;
                        default:
                    }
                    return (startEnd[0] != null && startEnd[1] != null)
                            ? entry(transactionId, startEnd[1].timestamp() - startEnd[0].timestamp())
                            : null;
                },
                (startEnd, transactionId, wm) -> // <4>
                    (startEnd[0] == null || startEnd[1] == null)
                        ? entry(transactionId, TIMED_OUT)
                        : null
            );
        //end::s20[]
    }

    static void s21() {
        StreamStage<String> stage = null;
        AggregateOperation1<String, LongAccumulator, String> aggrOp = null;
        //tag::s21[]
        stage.mapStateful(aggrOp.createFn(), (acc, item) -> {
            aggrOp.accumulateFn().accept(acc, item);
            return aggrOp.exportFn().apply(acc);
        });
        //end::s21[]
    }

    private static class TransactionEvent {
        int type() { return 0; }
        String transactionId() { return ""; }
        long timestamp() { return 0; }
    }

    private static StreamStage<TransactionEvent> eventStream() {
        throw new UnsupportedOperationException();
    }


    static void apply() {
        Pipeline p = Pipeline.create();
        BatchSource<String> source = null;
        {
            //tag::apply2[]
            BatchStage<String> stage = p.readFrom(source);
            BatchStage<String> cleanedUp = PipelineTransforms.cleanUp(stage);
            BatchStage<Long> counted = cleanedUp.aggregate(counting());
            //end::apply2[]
        }
        //tag::apply3[]
        BatchStage<Long> counted = p.readFrom(source)
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

    static void retres1() {
        //tag::retres1[]
        JetInstance jet = Jet.newJetInstance();
        Observable<Long> observable = jet.newObservable();
        observable.addObserver(System.out::println);

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .writeTo(Sinks.observable(observable));

        jet.newJob(pipeline).join();

        observable.destroy();
        //end::retres1[]
    }

    static void retres2() {
        //tag::retres2[]
        JetInstance jet = Jet.newJetInstance();
        Observable<Long> observable = jet.getObservable("results");
        observable.addObserver(System.out::println);

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .writeTo(Sinks.observable("results"));

        jet.newJob(pipeline).join();

        observable.destroy();
        //end::retres2[]
    }

    static void retres3() {
        //tag::retres3[]
        JetInstance jet = Jet.newJetInstance();
        Observable<Long> observable = jet.getObservable("results");

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .writeTo(Sinks.observable("results"));

        observable.toFuture(Stream::count).thenAccept(System.out::println);
        jet.newJob(pipeline).join();

        observable.destroy();
        //end::retres3[]
    }
}

