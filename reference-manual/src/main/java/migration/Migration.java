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

package migration;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.test.SimpleEvent;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;

import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class Migration {

    static void config() {
        ClientConfig clientConfig = new ClientConfig();
        //tag::config1[]
        clientConfig.setClusterName("cluster_name");
        //clientConfig.getGroupConfig().setName("cluster_name")
        //end::config1[]

        Config config = new Config();
        //tag::config2[]
        config.getMapConfig("map_name").getEventJournalConfig();
        //config.getMapEventJournalConfig("map_name")
        //end::config2[]

        JetConfig jetConfig = new JetConfig();
        //tag::config3[]
        jetConfig.getHazelcastConfig().getMetricsConfig().setCollectionFrequencySeconds(1);
        //jetConfig.getMetricsConfig().setCollectionIntervalSeconds(1);
        //end::config3[]
    }

    static void pipeline() {
        Pipeline pipeline = Pipeline.create();
        //tag::pipeline1[]
        pipeline.readFrom(TestSources.items(1, 2, 3)).writeTo(Sinks.logger());
        //pipeline.drawFrom(TestSources.items(1, 2, 3)).drainTo(Sinks.logger());
        //end::pipeline1[]

        //tag::pipeline2[]
        pipeline.readFrom(TestSources.items(1, 2, 3))
                .filterUsingService(
                        ServiceFactories.sharedService(pctx -> 1),
                        (svc, i) -> i % 2 == svc)
                .writeTo(Sinks.logger());

        /*
        pipeline.drawFrom(TestSources.items(1, 2, 3))
                .filterUsingContext(
                        ContextFactory.withCreateFn(i -> 1),
                        (ctx, i) -> i % 2 == ctx)
                .drainTo(Sinks.logger());
        */
        //end::pipeline2[]
    }

    static void entryProcessor() {
        JetInstance jet = Jet.newJetInstance();
        IMap<Object, Object> map = jet.getMap("map");

        //tag::entryProcessor1[]
        FunctionEx<Map.Entry<String, Integer>, EntryProcessor<String, Integer, Void>> entryProcFn =
                entry ->
                        (EntryProcessor<String, Integer, Void>) e -> {
                            e.setValue(e.getValue() == null ? 1 : e.getValue() + 1);
                            return null;
                        };
        Sinks.mapWithEntryProcessor(map, Map.Entry::getKey, entryProcFn);

        /*
        FunctionEx<Map.Entry<String, Integer>, EntryProcessor<String, Integer>> entryProcFn =
                entry ->
                        (EntryProcessor<String, Integer>) e -> {
                            e.setValue(e.getValue() == null ? 1 : e.getValue() + 1);
                            return null;
                        };
        Sinks.mapWithEntryProcessor(map, Map.Entry::getKey, entryProcFn);
        */
        //end::entryProcessor1[]
    }

    static void serviceFactory() {
        //tag::serviceFactory1[]
        ServiceFactories.sharedService(ctx -> Executors.newFixedThreadPool(8), ExecutorService::shutdown);
        //ContextFactory.withCreateFn(jet -> Executors.newFixedThreadPool(8)).withLocalSharing();

        ServiceFactories.nonSharedService(ctx -> DateTimeFormatter.ofPattern("HH:mm:ss.SSS"), ConsumerEx.noop());
        //ContextFactory.withCreateFn(jet -> DateTimeFormatter.ofPattern("HH:mm:ss.SSS"))
        //end::serviceFactory1[]

        Pipeline pipeline = Pipeline.create();
        BatchStage<Runnable> stage = pipeline.readFrom(TestSources.items(1, 2, 3)).map(i -> (Runnable) () -> {});

        //tag::serviceFactory2[]
        stage.mapUsingServiceAsync(
                ServiceFactories.sharedService(ctx -> Executors.newFixedThreadPool(8)),
                2,
                false,
                (exec, task) -> CompletableFuture.supplyAsync(() -> task, exec)
        );

        /*
        stage.mapUsingContextAsync(
                ContextFactory.withCreateFn(jet -> Executors.newFixedThreadPool(8))
                        .withMaxPendingCallsPerProcessor(2)
                        .withUnorderedAsyncResponses(),
                (exec, task) -> CompletableFuture.supplyAsync(() -> task, exec)
        );
        */
        //end::serviceFactory2[]
    }

    static void async() {
        Pipeline p = Pipeline.create();
        ServiceFactory<?, ExecutorService> serviceFactory = ServiceFactories.sharedService(ctx -> Executors.newFixedThreadPool(8));

        StreamStage<Long> stage = p.readFrom(TestSources.itemStream(3))
                .withoutTimestamps()
                .map(SimpleEvent::sequence);

        //tag::async1[]
        stage.mapUsingServiceAsync(serviceFactory,
                (executor, item) -> {
                    CompletableFuture<List<String>> f = new CompletableFuture<>();
                    executor.submit(() -> f.complete(Arrays.asList(item + "-1", item + "-2", item + "-3")));
                    return f;
                })
                .flatMap(Traversers::traverseIterable);
        /*
        stage.flatMapUsingServiceAsync(serviceFactory,
                (executor, item) -> {
                    CompletableFuture<Traverser<String>> f = new CompletableFuture<>();
                    executor.submit(() -> f.complete(traverseItems(item + "-1", item + "-2", item + "-3")));
                    return f;
                })
        */
        //end::async1[]

        //tag::async2[]
        stage.mapUsingServiceAsync(serviceFactory,
                (executor, item) -> {
                    CompletableFuture<Long> f = new CompletableFuture<>();
                    executor.submit(() -> f.complete(item % 2 == 0 ? item : null));
                    return f;
                });
        /*
        stage.filterUsingServiceAsync(serviceFactory,
                (executor, item) -> {
                    CompletableFuture<Boolean> f = new CompletableFuture<>();
                    executor.submit(() -> f.complete(item % 2 == 0));
                    return f;
                });
        */
        //end::async2[]
    }

}
