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

import com.google.common.collect.ImmutableMap;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.search.SearchHit;
import org.junit.After;
import org.junit.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.elastic.ElasticClients.client;
import static com.hazelcast.jet.elastic.ElasticSupport.PORT;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class AuthElasticSourcesTest extends BaseElasticTest {

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void afterClass() {
        factory.terminateAll();
    }

    @Override protected SupplierEx<RestClientBuilder> elasticClientSupplier() {
        return ElasticSupport.secureElasticClientSupplier();
    }

    @Override
    protected HazelcastInstance createHazelcastInstance() {
        return factory.newHazelcastInstance(config());
    }

    @Test
    public void given_authenticatedClient_whenReadFromElasticSource_thenFinishSuccessfully() {
        indexDocument("my-index", ImmutableMap.of("name", "Frantisek"));

        Pipeline p = Pipeline.create();
        p.readFrom(elasticSource(elasticClientSupplier()))
         .writeTo(Sinks.list(results));

        submitJob(p);
    }

    @Test
    public void given_clientWithWrongPassword_whenReadFromElasticSource_thenFailWithAuthenticationException() {
        ElasticsearchContainer container = ElasticSupport.secureElastic.get();
        String containerIp = container.getContainerIpAddress();
        Integer port = container.getMappedPort(PORT);

        Pipeline p = Pipeline.create();
        p.readFrom(elasticSource(() -> client("elastic", "WrongPassword", containerIp, port)))
         .writeTo(Sinks.list(results));

        assertThatThrownBy(() -> submitJob(p))
                .hasRootCauseInstanceOf(ResponseException.class)
                .hasStackTraceContaining("failed to authenticate user [elastic]");
    }

    @Test
    public void given_clientWithoutAuthentication_whenReadFromElasticSource_then_failWithAuthenticationException() {
        ElasticsearchContainer container = ElasticSupport.secureElastic.get();
        String containerIp = container.getContainerIpAddress();
        Integer port = container.getMappedPort(PORT);

        Pipeline p = Pipeline.create();
        p.readFrom(elasticSource(() -> client(containerIp, port)))
         .writeTo(Sinks.list(results));

        assertThatThrownBy(() -> submitJob(p))
                .hasRootCauseInstanceOf(ResponseException.class)
                .hasStackTraceContaining("missing authentication token");
    }

    @Nonnull
    private BatchSource<String> elasticSource(SupplierEx<RestClientBuilder> clientFn) {
        return ElasticSources.builder()
                             .clientFn(clientFn)
                             .searchRequestFn(SearchRequest::new)
                             .mapToItemFn(SearchHit::getSourceAsString)
                             .retries(0)
                             .build();
    }
}
