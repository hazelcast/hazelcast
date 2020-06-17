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

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.impl.util.Util;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.util.function.Supplier;

import static com.hazelcast.jet.elastic.ElasticClients.client;

public final class ElasticSupport {

    public static final String ELASTICSEARCH_IMAGE = "elasticsearch:6.8.9";
    public static final int PORT = 9200;

    // Elastic container takes long time to start up, reusing the container for speedup
    public static final Supplier<ElasticsearchContainer> elastic = Util.memoize(() -> {
        ElasticsearchContainer elastic = new ElasticsearchContainer(ELASTICSEARCH_IMAGE);
        elastic.start();
        Runtime.getRuntime().addShutdownHook(new Thread(elastic::stop));
        return elastic;
    });

    /**
     * Using elastic container configured with security enabled
     */
    public static Supplier<ElasticsearchContainer> secureElastic = Util.memoize(() -> {
        ElasticsearchContainer elastic = new ElasticsearchContainer(ELASTICSEARCH_IMAGE)
                .withEnv("ELASTIC_USERNAME", "elastic")
                .withEnv("ELASTIC_PASSWORD", "SuperSecret")
                .withEnv("xpack.security.enabled", "true");

        elastic.start();
        Runtime.getRuntime().addShutdownHook(new Thread(elastic::stop));
        return elastic;
    });

    private ElasticSupport() {
    }

    public static SupplierEx<RestClientBuilder> elasticClientSupplier() {
        String address = elastic.get().getHttpHostAddress();
        return () -> RestClient.builder(HttpHost.create(address));
    }

    public static SupplierEx<RestClientBuilder> secureElasticClientSupplier() {
        ElasticsearchContainer container = elastic.get();
        String containerIp = container.getContainerIpAddress();
        Integer port = container.getMappedPort(PORT);

        return () -> client("elastic", "SuperSecret", containerIp, port);
    }

}
