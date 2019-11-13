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

package integration;

import com.hazelcast.cache.EventJournalCacheEvent;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.collection.IList;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.examples.enrichment.datamodel.Person;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSourceStage;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.EventJournalMapEvent;

import java.util.ArrayList;
import java.util.Map.Entry;

import static com.hazelcast.function.FunctionEx.identity;
import static com.hazelcast.function.PredicateEx.alwaysTrue;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_CURRENT;

public class ImdgConnectors {
    static void s1() {
        //tag::s1[]
        Pipeline p = Pipeline.create();
        BatchStage<Entry<String, Long>> stage = p.readFrom(Sources.map("inMap"));
        stage.writeTo(Sinks.map("outMap"));
        //end::s1[]
    }

    static void s2() {
        //tag::s2[]
        Pipeline p = Pipeline.create();
        BatchStage<Entry<String, Long>> stage = p.readFrom(Sources.cache("inCache"));
        stage.writeTo(Sinks.cache("outCache"));
        //end::s2[]
    }

    static void s3() {
        //tag::s3[]
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(Sources.<String, Long>map("inMap"))
                .writeTo(Sinks.mapWithMerging("outMap",
                        Entry::getKey,
                        Entry::getValue,
                        (oldValue, newValue) -> oldValue + newValue)
                );
        //end::s3[]
    }

    static void s4() {
        //tag::s4[]
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(Sources.<String, Long>map("inMap"))
            .writeTo(Sinks.<Entry<String, Long>, String, Long>mapWithUpdating(
                "outMap", Entry::getKey,
                (oldV, item) -> (oldV != null ? oldV : 0L) + item.getValue())
            );
        //end::s4[]
    }

    static void s5() {
    //tag::s5[]
    Pipeline pipeline = Pipeline.create();
    pipeline.readFrom(Sources.<String, Integer>map("mymap"))
            .writeTo(Sinks.mapWithEntryProcessor("mymap",
                    Entry::getKey,
                    entry -> new IncrementEntryProcessor(5)
            ));

    //end::s5[]
    }

    static
    //tag::s6[]
    class IncrementEntryProcessor implements EntryProcessor<String, Integer, Integer> {

        private int incrementBy;

        public IncrementEntryProcessor(int incrementBy) {
            this.incrementBy = incrementBy;
        }

        @Override
        public Integer process(Entry<String, Integer> entry) {
            return entry.setValue(entry.getValue() + incrementBy);
        }

    }
    //end::s6[]

    static void s7() {
        //tag::s7[]
        ClientConfig cfg = new ClientConfig();
        cfg.setClusterName("myGroup");
        cfg.getNetworkConfig().addAddress("node1.mydomain.com", "node2.mydomain.com");

        Pipeline p = Pipeline.create();
        BatchStage<Entry<String, Long>> fromMap =
                p.readFrom(Sources.remoteMap("inputMap", cfg));
        BatchStage<Entry<String, Long>> fromCache =
                p.readFrom(Sources.remoteCache("inputCache", cfg));
        fromMap.writeTo(Sinks.remoteCache("outputCache", cfg));
        fromCache.writeTo(Sinks.remoteMap("outputMap", cfg));
        //end::s7[]
    }

    static void s8() {
        ClientConfig clientConfig = new ClientConfig();
        //tag::s8[]
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.<Integer, String, Person>remoteMap(
                "inputMap", clientConfig,
                e -> e.getValue().getAge() > 21,
                e -> e.getValue().getAge()));
        //end::s8[]
    }

    static void s9() {
        //tag::s9[]
        JetConfig cfg = new JetConfig();
        cfg.getHazelcastConfig()
           .getMapConfig("inputMap")
           .getEventJournalConfig()
           .setEnabled(true)
           .setCapacity(1000)         // how many events to keep before evicting
           .setTimeToLiveSeconds(10); // evict events older than this
        JetInstance jet = Jet.newJetInstance(cfg);
        //end::s9[]

        //tag::s10[]
        cfg.getHazelcastConfig()
           .getCacheConfig("inputCache")
           .getEventJournalConfig()
           .setEnabled(true)
           .setCapacity(1000)
           .setTimeToLiveSeconds(10);
        //end::s10[]
    }

    static void s11() {
        //tag::s11[]
        Pipeline p = Pipeline.create();
        StreamSourceStage<Entry<String, Long>> fromMap = p.readFrom(
                Sources.mapJournal("inputMap", START_FROM_CURRENT));
        StreamSourceStage<Entry<String, Long>> fromCache = p.readFrom(
                Sources.cacheJournal("inputCache", START_FROM_CURRENT));
        //end::s11[]
    }

    static void s12() {
        //tag::s12[]
        Pipeline p = Pipeline.create();
        StreamSourceStage<EventJournalMapEvent<String, Long>> allFromMap = p.readFrom(
            Sources.mapJournal("inputMap",
                    START_FROM_CURRENT, identity(), alwaysTrue()));
        StreamSourceStage<EventJournalCacheEvent<String, Long>> allFromCache = p.readFrom(
            Sources.cacheJournal("inputMap",
                    START_FROM_CURRENT, identity(), alwaysTrue()));
        //end::s12[]
    }

    static void s13() {
        ClientConfig someClientConfig = new ClientConfig();
        //tag::s13[]
        Pipeline p = Pipeline.create();
        StreamSourceStage<Entry<String, Long>> fromRemoteMap = p.readFrom(
            Sources.remoteMapJournal("inputMap",
                    someClientConfig, START_FROM_CURRENT));
        StreamSourceStage<Entry<String, Long>> fromRemoteCache = p.readFrom(
            Sources.remoteCacheJournal("inputCache",
                    someClientConfig, START_FROM_CURRENT));
        //end::s13[]
    }

    static void s14() {
        JetInstance jet = Jet.newJetInstance();
        //tag::s14[]
        IList<Integer> inputList = jet.getList("inputList");
        for (int i = 0; i < 10; i++) {
            inputList.add(i);
        }

        Pipeline p = Pipeline.create();
        p.readFrom(Sources.<Integer>list("inputList"))
         .map(i -> "item" + i)
         .writeTo(Sinks.list("resultList"));

        jet.newJob(p).join();

        IList<String> resultList = jet.getList("resultList");
        System.out.println("Results: " + new ArrayList<>(resultList));
        //end::s14[]
    }

    static void s15() {
        //tag::s15[]
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("myGroup");
        clientConfig.getNetworkConfig()
                    .addAddress("node1.mydomain.com", "node2.mydomain.com");

        Pipeline p = Pipeline.create();
        p.readFrom(Sources.remoteList("inputlist", clientConfig))
         .writeTo(Sinks.remoteList("outputList", clientConfig));
        //end::s15[]
    }
}
