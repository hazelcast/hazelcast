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

import com.hazelcast.function.SupplierEx;
import org.apache.http.HttpHost;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import javax.annotation.Nonnull;

import static org.apache.http.auth.AuthScope.ANY;

/**
 * Collection of convenience factory methods for Elastic's {@link RestClientBuilder}
 * <p>
 * Supposed to be used as a parameter to {@link ElasticSourceBuilder#clientFn(SupplierEx)}
 * and {@link ElasticSinkBuilder#clientFn(SupplierEx)}, for example:
 * <pre>{@code
 * builder.clientFn(() -> client());
 * }</pre>
 */
public final class ElasticClients {

    private static final int DEFAULT_PORT = 9200;

    private ElasticClients() {
    }

    /**
     * Create Elastic client for an instance running on localhost
     * on default port (9200)
     */
    @Nonnull
    public static RestClientBuilder client() {
        return client("localhost", DEFAULT_PORT);
    }

    /**
     * Convenience method to create {@link RestClientBuilder} with given string, it must contain host, and optionally
     * the scheme and a port.
     *
     * Valid examples:
     * <pre>{@code elastic-host
     * elastic-host:9200
     * http://elastic-host:9200}</pre>
     *
     * @see HttpHost#create(String)
     * @since Jet 4.3
     */
    @Nonnull
    public static RestClientBuilder client(@Nonnull String location) {
        return RestClient.builder(HttpHost.create(location));
    }

    /**
     * Convenience method to create {@link RestClientBuilder} with given
     * hostname and port
     */
    @Nonnull
    public static RestClientBuilder client(@Nonnull String hostname, int port) {
        return RestClient.builder(new HttpHost(hostname, port));
    }

    /**
     * Convenience method to create {@link RestClientBuilder} with basic authentication
     * and given hostname and port
     * <p>
     * Usage:
     * <pre>{@code
     * BatchSource<SearchHit> source = elastic(() -> client("user", "password", "host", 9200));
     * }</pre>
     */
    @Nonnull
    public static RestClientBuilder client(
            @Nonnull String username,
            @Nonnull String password,
            @Nonnull String hostname,
            int port
    ) {
        return client(username, password, hostname, port, "http");
    }
    /**
     * Convenience method to create {@link RestClientBuilder} with basic authentication
     * and given hostname, port and scheme. Valid schemes are "http" and "https".
     * <p>
     * Usage:
     * <pre>{@code
     * BatchSource<SearchHit> source = elastic(() -> client("user", "password", "host", 9200, "https"));
     * }</pre>
     */
    @Nonnull
    public static RestClientBuilder client(
            @Nonnull String username,
            @Nonnull String password,
            @Nonnull String hostname,
            int port,
            @Nonnull String scheme
    ) {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(ANY, new UsernamePasswordCredentials(username, password));
        return RestClient.builder(new HttpHost(hostname, port, scheme))
                         .setHttpClientConfigCallback(httpClientBuilder ->
                                 httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                         );
    }
}
