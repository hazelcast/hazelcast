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

import com.google.common.collect.ImmutableMap;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClientBuilder;
import org.junit.After;
import org.junit.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import static com.hazelcast.jet.elastic.ElasticClients.client;
import static com.hazelcast.jet.elastic.ElasticSupport.PORT;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class AuthElasticSourcesTest extends BaseElasticTest {

    private final JetTestInstanceFactory factory = new JetTestInstanceFactory();

    @After
    public void afterClass() throws Exception {
        factory.terminateAll();
    }

    @Override protected SupplierEx<RestClientBuilder> elasticClientSupplier() {
        return ElasticSupport.secureElasticClientSupplier();
    }

    @Override
    protected JetInstance createJetInstance() {
        return factory.newMember(new JetConfig());
    }

    @Test
    public void given_authenticatedClient_whenReadFromElasticSource_thenFinishSuccessfully() {
        indexDocument("my-index", ImmutableMap.of("name", "Frantisek"));

        Pipeline p = Pipeline.create();
        p.readFrom(ElasticSources.elastic(elasticClientSupplier()))
         .writeTo(Sinks.list(results));

        submitJob(p);
    }

    @Test
    public void given_clientWithWrongPassword_whenReadFromElasticSource_thenFailWithAuthenticationException() {
        ElasticsearchContainer container = ElasticSupport.secureElastic.get();
        String containerIp = container.getContainerIpAddress();
        Integer port = container.getMappedPort(PORT);

        Pipeline p = Pipeline.create();
        p.readFrom(ElasticSources.elastic(() -> client("elastic", "WrongPassword", containerIp, port)))
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
        p.readFrom(ElasticSources.elastic(() -> client(containerIp, port)))
         .writeTo(Sinks.list(results));

        assertThatThrownBy(() -> submitJob(p))
                .hasRootCauseInstanceOf(ResponseException.class)
                .hasStackTraceContaining("missing authentication token");
    }
}
