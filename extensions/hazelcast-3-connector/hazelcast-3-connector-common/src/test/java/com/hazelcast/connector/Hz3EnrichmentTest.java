/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.connector;

import com.hazelcast.collection.IList;
import com.hazelcast.connector.map.AsyncMap;
import com.hazelcast.connector.map.Hz3MapAdapter;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.map.IMap;
import com.hazelcast.replicatedmap.ReplicatedMap;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.connector.Hz3Enrichment.hz3MapServiceFactory;
import static com.hazelcast.connector.Hz3Enrichment.hz3ReplicatedMapServiceFactory;
import static com.hazelcast.connector.Hz3Enrichment.mapUsingIMap;
import static com.hazelcast.connector.Hz3Enrichment.mapUsingIMapAsync;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class Hz3EnrichmentTest extends BaseHz3Test {

    @Test
    public void testMapUsingIMap() {
        IMap<Object, Object> map = hz3.getMap("test-map");
        map.put(1, "a");
        map.put(2, "b");

        HazelcastInstance hz = createHazelcastInstance();
        IList<String> results = hz.getList("result-list");
        Pipeline p = Pipeline.create();

        ServiceFactory<Hz3MapAdapter, AsyncMap<Integer, String>> hz3MapSF =
                hz3MapServiceFactory("test-map", HZ3_CLIENT_CONFIG);

        BiFunctionEx<? super Map<Integer, String>, ? super Integer, String> mapFn =
                mapUsingIMap(FunctionEx.identity(), (Integer i, String s) -> s);
        BatchStage<String> mapStage = p.readFrom(TestSources.items(1, 2, 3))
                .mapUsingService(
                        hz3MapSF,
                        mapFn
                );
        mapStage.writeTo(Sinks.list(results));

        JobConfig config = getJobConfig(mapStage.name());
        hz.getJet().newJob(p, config).join();
        assertThat(results).containsOnly("a", "b");
    }

    @Test
    public void testMapUsingIMapAsync() {
        IMap<Object, Object> map = hz3.getMap("test-map");
        map.put(1, "a");
        map.put(2, "b");

        HazelcastInstance hz = createHazelcastInstance();
        IList<String> results = hz.getList("result-list");
        Pipeline p = Pipeline.create();

        ServiceFactory<Hz3MapAdapter, AsyncMap<Integer, String>> hz3MapSF =
                hz3MapServiceFactory("test-map", HZ3_CLIENT_CONFIG);

        BiFunctionEx<? super AsyncMap<Integer, String>, ? super Integer, CompletableFuture<String>> mapFn =
                mapUsingIMapAsync(FunctionEx.identity(), (Integer i, String s) -> s);

        BatchStage<String> mapStage = p.readFrom(TestSources.items(1, 2, 3))
                .mapUsingServiceAsync(
                        hz3MapSF,
                        mapFn
                );
        mapStage.writeTo(Sinks.list(results));

        JobConfig config = getJobConfig(mapStage.name());
        hz.getJet().newJob(p, config).join();
        assertThat(results).containsOnly("a", "b");
    }

    @Test
    public void testMapUsingReplicatedMap() {
        ReplicatedMap<Object, Object> map = hz3.getReplicatedMap("test-replicated-map");
        map.put(1, "a");
        map.put(2, "b");

        HazelcastInstance hz = createHazelcastInstance();
        IList<String> results = hz.getList("result-list");
        Pipeline p = Pipeline.create();

        ServiceFactory<Hz3MapAdapter, Map<Integer, String>> hz3MapSF =
                hz3ReplicatedMapServiceFactory("test-replicated-map", HZ3_CLIENT_CONFIG);

        BiFunctionEx<? super Map<Integer, String>, ? super Integer, String> mapFn =
                mapUsingIMap(FunctionEx.identity(), (Integer i, String s) -> s);

        BatchStage<String> mapStage = p.readFrom(TestSources.items(1, 2, 3))
                .mapUsingService(
                        hz3MapSF,
                        mapFn
                );
        mapStage.writeTo(Sinks.list(results));

        JobConfig config = getJobConfig(mapStage.name());
        hz.getJet().newJob(p, config).join();
        assertThat(results).containsOnly("a", "b");
    }

    @Test
    public void when_enrichFromInstanceDown_then_shouldThrowJetException() {
        HazelcastInstance hz = createHazelcastInstance();
        IList<String> results = hz.getList("result-list");
        Pipeline p = Pipeline.create();

        ServiceFactory<Hz3MapAdapter, AsyncMap<Integer, String>> hz3MapSF =
                hz3MapServiceFactory("test-map", HZ3_DOWN_CLIENT_CONFIG);

        BiFunctionEx<? super Map<Integer, String>, ? super Integer, String> mapFn =
                mapUsingIMap(FunctionEx.identity(), (Integer i, String s) -> s);
        BatchStage<String> mapStage = p.readFrom(TestSources.items(1, 2, 3))
                                       .mapUsingService(
                                               hz3MapSF,
                                               mapFn
                                       );
        mapStage.writeTo(Sinks.list(results));

        JobConfig config = getJobConfig(mapStage.name());
        Job job = hz.getJet().newJob(p, config);

        assertThatThrownBy(() -> job.join())
                .hasStackTraceContaining(JetException.class.getName())
                .hasStackTraceContaining("Unable to connect to any cluster");
    }
}
