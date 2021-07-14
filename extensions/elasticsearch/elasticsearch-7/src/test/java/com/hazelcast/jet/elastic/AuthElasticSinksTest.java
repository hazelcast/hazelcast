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
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.elastic.CommonElasticSinksTest.TestItem;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.RestClientBuilder;
import org.junit.After;
import org.junit.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.io.IOException;

import static com.hazelcast.jet.elastic.ElasticClients.client;
import static com.hazelcast.jet.elastic.ElasticSupport.PORT;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class AuthElasticSinksTest extends BaseElasticTest {

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void afterClass() {
        factory.terminateAll();
    }

    @Override
    protected SupplierEx<RestClientBuilder> elasticClientSupplier() {
        return ElasticSupport.secureElasticClientSupplier();
    }

    @Override
    protected HazelcastInstance createHazelcastInstance() {
        return factory.newHazelcastInstance(config());
    }

    @Test
    public void given_authenticatedClient_whenWriteToElasticSink_thenFinishSuccessfully() throws IOException {
        Sink<TestItem> elasticSink = new ElasticSinkBuilder<>()
                .clientFn(elasticClientSupplier())
                .bulkRequestFn(() -> new BulkRequest().setRefreshPolicy(RefreshPolicy.IMMEDIATE))
                .mapToRequestFn((TestItem item) -> new IndexRequest("my-index").source(item.asMap()))
                .build();

        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(new TestItem("id", "Frantisek")))
         .writeTo(elasticSink);

        submitJob(p);

        assertSingleDocument();
    }

    @Test
    public void given_clientWithWrongPassword_whenWriteToElasticSink_thenFailWithAuthenticationException() {
        ElasticsearchContainer container = ElasticSupport.secureElastic.get();
        String containerIp = container.getContainerIpAddress();
        Integer port = container.getMappedPort(PORT);

        Sink<TestItem> elasticSink = new ElasticSinkBuilder<>()
                .clientFn(() -> client("elastic", "WrongPassword", containerIp, port))
                .bulkRequestFn(() -> new BulkRequest().setRefreshPolicy(RefreshPolicy.IMMEDIATE))
                .mapToRequestFn((TestItem item) -> new IndexRequest("my-index").source(item.asMap()))
                .retries(0)
                .build();

        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(new TestItem("id", "Frantisek")))
         .writeTo(elasticSink);

        assertThatThrownBy(() -> submitJob(p))
                .hasRootCauseInstanceOf(ElasticsearchStatusException.class)
                .hasStackTraceContaining("unable to authenticate user [elastic]");
    }

    @Test
    public void given_clientWithoutAuthentication_whenWriteToElasticSink_thenFailWithAuthenticationException() {
        ElasticsearchContainer container = ElasticSupport.secureElastic.get();
        String containerIp = container.getContainerIpAddress();
        Integer port = container.getMappedPort(PORT);

        Sink<TestItem> elasticSink = new ElasticSinkBuilder<>()
                .clientFn(() -> client(containerIp, port))
                .bulkRequestFn(() -> new BulkRequest().setRefreshPolicy(RefreshPolicy.IMMEDIATE))
                .mapToRequestFn((TestItem item) -> new IndexRequest("my-index").source(item.asMap()))
                .retries(0)
                .build();

        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(new TestItem("id", "Frantisek")))
         .writeTo(elasticSink);

        assertThatThrownBy(() -> submitJob(p))
                .hasRootCauseInstanceOf(ElasticsearchStatusException.class)
                .hasStackTraceContaining("missing authentication credentials");
    }

}
