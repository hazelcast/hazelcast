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

package com.hazelcast.jet.elastic;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.ImmutableMap.of;
import static java.util.Collections.synchronizedList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test running single Jet member locally and Elastic in docker
 */
public class LocalElasticSourcesTest extends CommonElasticSourcesTest {

    private TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
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
                .hasCauseInstanceOf(JetException.class)
                .hasMessageContaining("Shard locations are not equal to Hazelcast members locations");
    }

    @Test
    public void when_readFromElasticSource_then_shouldCloseAllCreatedClients() throws IOException {
        indexDocument("my-index", of("name", "Frantisek"));

        Pipeline p = Pipeline.create();

        BatchSource<String> source = new ElasticSourceBuilder<>()
                .clientFn(() -> {
                    RestClientBuilder builder = spy(ElasticSupport.elasticClientSupplier().get());
                    when(builder.build()).thenAnswer(invocation -> {
                        Object result = invocation.callRealMethod();
                        RestClient elasticClient = (RestClient) spy(result);
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

        for (RestClient elasticClient : ClientHolder.elasticClients) {
            verify(elasticClient).close();
        }
    }

    static class ClientHolder implements Serializable {
        static List<RestClient> elasticClients = synchronizedList(new ArrayList<>());
    }
}
