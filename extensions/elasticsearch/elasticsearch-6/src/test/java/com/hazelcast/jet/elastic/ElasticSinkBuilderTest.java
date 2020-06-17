/*
 * Copyright 2020 Hazelcast Inc.
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

import com.hazelcast.jet.pipeline.PipelineTestSupport;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ElasticSinkBuilderTest extends PipelineTestSupport {

    @Test
    public void when_writeToFailingSink_then_shouldCloseClient() throws IOException {

        Sink<String> elasticSink = new ElasticSinkBuilder<>()
                .clientFn(() -> {
                    RestClientBuilder builder = spy(RestClient.builder(HttpHost.create("localhost:9200")));
                    when(builder.build()).thenAnswer(invocation -> {
                        Object result = invocation.callRealMethod();
                        ClientHolder.elasticClient = (RestClient) spy(result);
                        return ClientHolder.elasticClient;
                    });
                    return builder;
                })
                .bulkRequestFn(() -> new BulkRequest().setRefreshPolicy(RefreshPolicy.IMMEDIATE))
                .mapToRequestFn((String item) -> new IndexRequest("my-index"))
                .build();

        p.readFrom(TestSources.items("a", "b", "c"))
         .writeTo(elasticSink);

        try {
            execute();
        } catch (Exception e) {
            // ignore - elastic is not running
        }

        verify(ClientHolder.elasticClient).close();
    }

    static class ClientHolder implements Serializable {
        static RestClient elasticClient;
    }
}
