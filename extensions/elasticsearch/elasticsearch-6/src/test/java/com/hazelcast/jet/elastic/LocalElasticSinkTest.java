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
import com.hazelcast.jet.elastic.ElasticSinkBuilderTest.ClientHolder;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test running single Jet member locally and Elastic in docker
 */
public class LocalElasticSinkTest extends CommonElasticSinksTest {

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
    public void when_writeToSink_then_shouldCloseClient() throws IOException {
        ClientHolder.elasticClients.clear();

        Sink<String> elasticSink = new ElasticSinkBuilder<>()
                .clientFn(() -> {
                    RestClientBuilder builder = spy(RestClient.builder(HttpHost.create(
                            ElasticSupport.elastic.get().getHttpHostAddress()
                    )));
                    when(builder.build()).thenAnswer(invocation -> {
                        Object result = invocation.callRealMethod();
                        RestClient client = (RestClient) spy(result);
                        ClientHolder.elasticClients.add(client);
                        return client;
                    });
                    return builder;
                })
                .bulkRequestFn(() -> new BulkRequest().setRefreshPolicy(RefreshPolicy.IMMEDIATE))
                .mapToRequestFn((String item) -> new IndexRequest("my-index", "document").source(Collections.emptyMap()))
                .build();

        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items("a", "b", "c"))
         .writeTo(elasticSink);

        hz.getJet().newJob(p).join();

        for (RestClient client : ClientHolder.elasticClients) {
            verify(client).close();
        }
    }

}
