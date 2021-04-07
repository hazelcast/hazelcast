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

import com.hazelcast.config.Config;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
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
import org.junit.AfterClass;
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

    private JetTestInstanceFactory factory = new JetTestInstanceFactory();

    @After
    public void tearDown() throws Exception {
        factory.terminateAll();
    }

    @Override
    protected JetInstance createJetInstance() {
        // This starts very quickly, no need to cache the instance
        return factory.newMember(new Config());
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

        jet.newJob(p).join();

        for (RestClient client : ClientHolder.elasticClients) {
            verify(client).close();
        }
    }

    @AfterClass
    public static void afterClass() throws Exception {
        ClientHolder.elasticClients.clear();
    }
}
