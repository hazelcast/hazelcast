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

import com.hazelcast.core.IMap;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.BatchStageWithKey;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamSourceStage;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.StreamStageWithKey;
import datamodel.PageVisit;
import datamodel.Payment;
import datamodel.StockInfo;
import datamodel.Trade;

import java.util.List;
import java.util.Map.Entry;

import static com.hazelcast.jet.Traversers.traverseArray;
import static com.hazelcast.jet.Util.mapEventNewValue;
import static com.hazelcast.jet.Util.mapPutEvents;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.aggregate.AggregateOperations.maxBy;
import static com.hazelcast.jet.aggregate.AggregateOperations.toList;
import static com.hazelcast.jet.function.ComparatorEx.comparing;
import static com.hazelcast.jet.function.Functions.entryValue;
import static com.hazelcast.jet.function.Functions.wholeItem;
import static com.hazelcast.jet.pipeline.JoinClause.joinMapEntries;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_CURRENT;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.pipeline.Sources.list;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;

public class CheatSheet {
    static Pipeline p;

    static void s1() {
        //tag::s1[]
        BatchStage<String> lines = p.drawFrom(list("lines"));
        BatchStage<String> lowercased = lines.map(String::toLowerCase);
        //end::s1[]
    }

    static void s2() {
        //tag::s2[]
        BatchStage<String> lines = p.drawFrom(list("lines"));
        BatchStage<String> nonEmpty = lines.filter(string -> !string.isEmpty());
        //end::s2[]
    }

    static void s3() {
        //tag::s3[]
        BatchStage<String> lines = p.drawFrom(list("lines"));
        BatchStage<String> words = lines.flatMap(
                line -> traverseArray(line.split("\\W+")));
        //end::s3[]
    }

    static void s4() {
        //tag::s4[]
        BatchStage<Trade> trades = p.drawFrom(list("trades"));
        BatchStage<Entry<String, StockInfo>> stockInfo =
                p.drawFrom(list("stockInfo"));
        BatchStage<Trade> joined = trades.hashJoin(stockInfo,
                joinMapEntries(Trade::ticker), Trade::setStockInfo);
        //end::s4[]
    }

    static void s4a() {
        JetInstance jet = Jet.newJetInstance();
        //tag::s4a[]
        StreamSource<Trade> tradesSource = Sources.mapJournal("trades",
                mapPutEvents(), mapEventNewValue(), START_FROM_CURRENT);
        IMap<String, StockInfo> stockMap = jet.getMap("stock-info");

        Pipeline p = Pipeline.create();
        p.drawFrom(tradesSource)
         .withoutTimestamps()
         .groupingKey(Trade::ticker)
         .mapUsingIMap(stockMap, Trade::setStockInfo)
         .drainTo(Sinks.list("result"));
        //end::s4a[]
    }

    static void s5() {
        //tag::s5[]
        BatchStage<String> lines = p.drawFrom(list("lines"));
        BatchStage<Long> count = lines.aggregate(counting());
        //end::s5[]
    }

    static void s6() {
        //tag::s6[]
        BatchStage<String> words = p.drawFrom(list("words"));
        BatchStage<Entry<String, Long>> wordsAndCounts =
                words.groupingKey(wholeItem())
                     .aggregate(counting());
        //end::s6[]
    }

    static void s7() {
        //tag::s7[]
        StreamSourceStage<Entry<Long, String>> tweetWords = p.drawFrom(
                Sources.mapJournal("tweet-words", START_FROM_OLDEST));
        StreamStage<KeyedWindowResult<String, Long>> wordFreqs =
                tweetWords.withTimestamps(e -> e.getKey(), 1000)
                          .window(sliding(1000, 10))
                          .groupingKey(entryValue())
                          .aggregate(counting());
        //end::s7[]
    }

    static void s8() {
        //tag::s8[]
        BatchStageWithKey<PageVisit, Integer> pageVisits =
                p.drawFrom(Sources.<PageVisit>list("pageVisit"))
                 .groupingKey(pageVisit -> pageVisit.userId());
        BatchStageWithKey<Payment, Integer> payments =
                p.drawFrom(Sources.<Payment>list("payment"))
                 .groupingKey(payment -> payment.userId());
        BatchStage<Entry<Integer, Tuple2<List<PageVisit>, List<Payment>>>>
            joined = pageVisits.aggregate2(toList(), payments, toList());
        //end::s8[]
    }

    static void s9() {
        //tag::s9[]
        StreamStageWithKey<PageVisit, Integer> pageVisits =
                p.<PageVisit>drawFrom(Sources.mapJournal("pageVisits",
                        mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST))
                        .withTimestamps(PageVisit::timestamp, 1000)
                        .groupingKey(PageVisit::userId);
        StreamStageWithKey<Payment, Integer> payments =
                p.<Payment>drawFrom(Sources.mapJournal("payments",
                        mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST))
                        .withTimestamps(Payment::timestamp, 1000)
                        .groupingKey(Payment::userId);
        StreamStage<KeyedWindowResult<Integer,
                                    Tuple2<List<PageVisit>, List<Payment>>>>
            joined = pageVisits.window(sliding(60_000, 1000))
                               .aggregate2(toList(), payments, toList());
        //end::s9[]
    }

    static void s10() {
        //tag::s10[]
        Pipeline p = Pipeline.create();
        StreamSource<Trade> tradesSource = Sources.mapJournal("trades",
                mapPutEvents(), mapEventNewValue(), START_FROM_CURRENT);
        StreamStage<Trade> currLargestTrade =
                p.drawFrom(tradesSource)
                 .withoutTimestamps()
                 .rollingAggregate(maxBy(comparing(Trade::worth)));
        //end::s10[]
    }

    static void s11() {
        //tag::s11[]
        BatchStage<String> strings = someStrings();
        BatchStage<String> distinctStrings = strings.distinct();
        BatchStage<String> distinctByPrefix =
                strings.groupingKey(s -> s.substring(0, 4)).distinct();
        //end::s11[]
    }

    private static BatchStage<String> someStrings() {
        throw new UnsupportedOperationException();
    }

    static void s12() {
        //tag::s12[]
        StreamStage<Trade> tradesNewYork = trades("new-york");
        StreamStage<Trade> tradesTokyo = trades("tokyo");
        StreamStage<Trade> tradesNyAndTokyo =
                tradesNewYork.merge(tradesTokyo);
        //end::s12[]
    }

    private static StreamStage<Trade> trades(String name) {
        throw new UnsupportedOperationException();
    }

    static void apply() {
        Pipeline p = Pipeline.create();
        BatchSource<String> source = null;
        //tag::apply1[]
        p.drawFrom(source)
         .map(String::toLowerCase)
         .filter(s -> s.startsWith("success"))
         .aggregate(counting())
        //end::apply1[]
        ;

        //tag::apply3[]
        p.drawFrom(source)
         .apply(PipelineTransforms::cleanUp)
         .aggregate(counting())
        //end::apply3[]
        ;
    }

    static class PipelineTransforms {
        //tag::apply2[]
        static BatchStage<String> cleanUp(BatchStage<String> input) {
            return input.map(String::toLowerCase)
                        .filter(s -> s.startsWith("success"));
        }
        //end::apply2[]
    }

    private static Traverser<String> fooFlatMap(String t) {
        return null;
    }

    private static String fooMap(String t) {
        return null;
    }

    //tag::custom-transform-1[]
    public static class IdentityMapP extends AbstractProcessor {
        protected boolean tryProcess(int ordinal, Object item) {
            return tryEmit(item);
        }
    }
    //end::custom-transform-1[]

    static void customTransform2() {
        Pipeline p = Pipeline.create();
        BatchSource<String> source = null;
        //tag::custom-transform-2[]
        p.drawFrom(source)
         .customTransform("name", IdentityMapP::new)
        //end::custom-transform-2[]
        ;
    }
}
