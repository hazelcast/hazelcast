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

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.SlidingWindowPolicy;
import com.hazelcast.jet.core.TimestampKind;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.ToLongFunctionEx;
import com.hazelcast.jet.pipeline.ContextFactory;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.map.journal.EventJournalMapEvent;
import datamodel.Trade;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.EventTimePolicy.eventTimePolicy;
import static com.hazelcast.jet.core.Partitioner.HASH_CODE;
import static com.hazelcast.jet.core.SlidingWindowPolicy.slidingWinPolicy;
import static com.hazelcast.jet.core.WatermarkPolicy.limitingLag;
import static com.hazelcast.jet.core.processor.Processors.mapUsingContextP;
import static com.hazelcast.jet.function.Functions.entryKey;
import static com.hazelcast.jet.function.PredicateEx.alwaysTrue;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;

public class StockExchangeCoreApi {

    private static final String TRADES_MAP_NAME = "trades";
    private static final String OUTPUT_DIR_NAME = "stock-exchange";
    private static final int SLIDING_WINDOW_LENGTH_MILLIS = 1000;
    private static final int SLIDE_STEP_MILLIS = 10;
    private static final int TRADES_PER_SECOND = 4_000;
    private static final int JOB_DURATION = 10;

    static DAG buildDag() {
//tag::s1[]
        ToLongFunctionEx<? super Trade> timestampFn = Trade::timestamp;
        FunctionEx<? super Trade, ?> keyFn = Trade::productId;
        SlidingWindowPolicy winPolicy = slidingWinPolicy(
                SLIDING_WINDOW_LENGTH_MILLIS, SLIDE_STEP_MILLIS);

        DAG dag = new DAG();
        Vertex tradeSource = dag.newVertex("trade-source",
                SourceProcessors.<Trade, Long, Trade>streamMapP(
                        TRADES_MAP_NAME,
                        alwaysTrue(),                              // <1>
                        EventJournalMapEvent::getNewValue,         // <1>
                        JournalInitialPosition.START_FROM_OLDEST,  // <2>
                        eventTimePolicy(
                                timestampFn,                       // <3>
                                limitingLag(SECONDS.toMillis(3)),  // <4>
                                winPolicy.frameSize(),             // <5>
                                winPolicy.frameOffset(),
                                SECONDS.toMillis(3)                // <6>
                        )));
        Vertex slidingStage1 = dag.newVertex("sliding-stage-1",
                Processors.accumulateByFrameP(
                        singletonList(keyFn),
                        singletonList(timestampFn),
                        TimestampKind.EVENT,
                        winPolicy, counting()
                ));
        Vertex slidingStage2 = dag.newVertex("sliding-stage-2",
            Processors.combineToSlidingWindowP(winPolicy, counting(),
                    KeyedWindowResult::new));
        Vertex formatOutput = dag.newVertex("format-output", mapUsingContextP(    // <7>
            ContextFactory.withCreateFn(x -> DateTimeFormatter.ofPattern("HH:mm:ss.SSS")),
            (DateTimeFormatter timeFormat, KeyedWindowResult<String, Long> kwr) ->
                String.format("%s %5s %4d",
                    timeFormat.format(Instant.ofEpochMilli(kwr.end())
                                             .atZone(ZoneId.systemDefault())),
                        kwr.getKey(), kwr.getValue())
        ));
        Vertex sink = dag.newVertex("sink", SinkProcessors.writeFileP(
                OUTPUT_DIR_NAME, Object::toString, UTF_8, false));

        tradeSource.localParallelism(1);

        return dag
                .edge(between(tradeSource, slidingStage1)
                        .partitioned(keyFn, HASH_CODE))
                .edge(between(slidingStage1, slidingStage2)
                        .partitioned(entryKey(), HASH_CODE)
                        .distributed())
                .edge(between(slidingStage2, formatOutput)
                        .isolated())
                .edge(between(formatOutput, sink));
//end::s1[]
    }
}
