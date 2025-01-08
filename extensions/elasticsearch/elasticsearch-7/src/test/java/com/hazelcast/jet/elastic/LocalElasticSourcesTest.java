/*
 * Copyright 2025 Hazelcast Inc.
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

package com.hazelcast.jet.elastic;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.test.IgnoreInJenkinsOnWindows;
import com.hazelcast.jet.test.SerialTest;
import com.hazelcast.test.annotation.NightlyTest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static java.util.Map.of;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Test running single Jet member locally and Elastic in docker
 */
@Category({NightlyTest.class, SerialTest.class, IgnoreInJenkinsOnWindows.class})
public class LocalElasticSourcesTest extends CommonElasticSourcesTest {

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    @Override
    public void tearDown() {
        factory.terminateAll();
    }

    @Override
    protected HazelcastInstance createHazelcastInstance() {
        // This starts very quickly, no need to cache the instance
        return factory.newHazelcastInstance(config());
    }

    @Test
    public void given_nonColocatedCluster_whenReadFromElasticSourceWithCoLocation_then_shouldThrowException() {
        indexDocument("my-index", of("name", "Frantisek"));

        Pipeline p = Pipeline.create();

        BatchSource<String> source = new ElasticSourceBuilder<>()
                .clientFn(elasticClientSupplier())
                .searchRequestFn(() -> new SearchRequest("my-index"))
                .mapToItemFn(hit -> (String) hit.getSourceAsMap().get("name"))
                .enableCoLocatedReading()
                .build();

        p.readFrom(source)
         .writeTo(Sinks.logger());

        assertThatThrownBy(() -> super.hz.getJet().newJob(p).join())
                .hasMessageContaining("Selected members do not contain shard 'my-index-0'");
    }

    @Test
    public void when_readFromElasticSource_then_shouldCloseAllCreatedClients() {
        ClientHolder.elasticClients.clear();

        indexDocument("my-index", of("name", "Frantisek"));

        Pipeline p = Pipeline.create();

        BatchSource<String> source = new ElasticSourceBuilder<>()
                .clientFn(() -> {
                    RestClientBuilder builder = spy(ElasticSupport.elasticClientSupplier().get());
                    when(builder.build()).thenAnswer(invocation -> {
                        Object result = invocation.callRealMethod();
                        RestClient elasticClient = (RestClient) result;
                        ClientHolder.elasticClients.add(elasticClient);
                        return elasticClient;
                    });
                    return builder;
                })
                .searchRequestFn(() -> new SearchRequest("my-index"))
                .mapToItemFn(hit -> (String) hit.getSourceAsMap().get("name"))
                .build();

        p.readFrom(source)
         .writeTo(Sinks.logger());

        submitJob(p);

        ClientHolder.assertAllClientsNotRunning();
    }
}
