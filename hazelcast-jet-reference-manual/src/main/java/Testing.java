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

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.jet.pipeline.test.Assertions;
import com.hazelcast.jet.pipeline.test.TestSources;

import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.core.test.JetAssert.assertEquals;
import static com.hazelcast.jet.core.test.JetAssert.assertTrue;

public class Testing {


    static void sources() {
        Pipeline pipeline = Pipeline.create();

        //tag::items[]
        pipeline.readFrom(TestSources.items(1, 2, 3, 4));
        //end::items[]

        //tag::items-collection[]
        List<Integer> list = IntStream.range(0, 1000)
              .boxed()
              .collect(Collectors.toList());
        pipeline.readFrom(TestSources.items(list))
                .writeTo(Sinks.logger());
        //end::items-collection[]

        //tag::items-stream[]
        pipeline.readFrom(TestSources.itemStream(10));
        //end::items-stream[]

        //tag::items-stream-trade[]
        pipeline.readFrom(TestSources.itemStream(10,
            (timestamp, sequence) -> new Trade(sequence, timestamp)))
                .withNativeTimestamps(0)
                .window(WindowDefinition.tumbling(1000))
                .aggregate(AggregateOperations.counting())
                .writeTo(Sinks.logger());
        //end::items-stream-trade[]
    }

    static void assertions() {
        Pipeline pipeline = Pipeline.create();

        //tag::assert-collected[]
        pipeline.readFrom(TestSources.items(1, 2, 3, 4))
                .apply(Assertions.assertCollected(list ->
                    assertEquals("4 items must be received", list.size(), 4))
                )
                .writeTo(Sinks.logger());
        //end::assert-collected[]

        //tag::assert-collected-eventually[]
        int itemsPerSecond = 10;
        pipeline.readFrom(TestSources.itemStream(itemsPerSecond))
                .withoutTimestamps()
                .apply(Assertions.assertCollectedEventually(10, list ->
                    assertTrue("At least 20 items must be received", list.size() > 20))
                )
                .writeTo(Sinks.logger());
        //end::assert-collected-eventually[]


        JetInstance jet = null;
        //tag::assertion-completed-exception[]
        try {
            jet.newJob(pipeline).join();
            fail("Job should have completed with an AssertionCompletedException," +
                " but instead completed normally"
            );
        } catch (CompletionException e) {
            assertContains(e.toString(), AssertionCompletedException.class.getName());
        }
        //end::assertion-completed-exception[]
    }

    private static void fail(String s) {
    }

    private static void assertContains(String actual, String expectedSubstring) {
    }

    private static class Trade {

        public Trade(long id, long timestamp) {

        }
    }
}
