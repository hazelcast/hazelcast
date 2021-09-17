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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.hazelcast.logging.ILogger;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.Serializable;

import static com.hazelcast.jet.elastic.impl.RetryUtils.withRetry;
import static com.hazelcast.jet.impl.util.Util.checkNonNullAndSerializable;
import static java.util.Objects.requireNonNull;

/**
 * Builder for Elasticsearch Sink
 * <p>
 * The Sink first maps items from the pipeline using the provided
 * {@link #mapToRequestFn(FunctionEx)} and then using {@link BulkRequest}.
 * <p>
 * {@link BulkRequest#BulkRequest()} is used by default, it can be
 * modified by providing custom {@link #bulkRequestFn(SupplierEx)}
 *
 * <p>
 * Usage:
 * <pre>{@code
 * Sink<Map<String, ?>> elasticSink = new ElasticSinkBuilder<Map<String, ?>>()
 *   .clientFn(() -> ElasticClients.client(host, port))
 *   .mapToRequestFn(item -> new IndexRequest("my-index").source(item))
 *   .build();
 * }</pre>
 * <p>
 * Requires {@link #clientFn(SupplierEx)} and {@link #mapToRequestFn(FunctionEx)}.
 *
 * @param <T>
 * @since Jet 4.2
 */
public final class ElasticSinkBuilder<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String DEFAULT_NAME = "elasticSink";
    private static final int DEFAULT_LOCAL_PARALLELISM = 2;
    private static final int DEFAULT_RETRIES = 5;

    private SupplierEx<RestClientBuilder> clientFn;
    private SupplierEx<BulkRequest> bulkRequestFn = BulkRequest::new;
    private FunctionEx<? super T, ? extends DocWriteRequest<?>> mapToRequestFn;
    private FunctionEx<? super ActionRequest, RequestOptions> optionsFn = (request) -> RequestOptions.DEFAULT;
    private int retries = DEFAULT_RETRIES;

    /**
     * Set the client supplier function
     * <p>
     * The connector uses the returned instance to access Elasticsearch.
     * Also see {@link ElasticClients} for convenience
     * factory methods.
     * <p>
     * For example, to provide an authenticated client:
     * <pre>{@code
     * builder.clientFn(() -> client(host, port, username, password))
     * }</pre>
     *
     * This parameter is required.
     *
     * @param clientFn supplier function returning configured Elasticsearch
     *                 REST client
     */
    @Nonnull
    public ElasticSinkBuilder<T> clientFn(@Nonnull SupplierEx<RestClientBuilder> clientFn) {
        this.clientFn = checkNonNullAndSerializable(clientFn, "clientFn");
        return this;
    }

    /**
     * Set the supplier function for BulkRequest, defaults to new {@link BulkRequest#BulkRequest()}
     * <p>
     * For example, to modify the BulkRequest used to index documents:
     * <pre>{@code
     * builder.bulkRequestFn(() -> new BulkRequest().setRefreshPolicy(IMMEDIATE))
     * }</pre>
     *
     * @param bulkRequestFn supplier function for the bulk request
     * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-update-settings.html#bulk">
     * Bulk indexing usage in Elastic documentation</a>
     */
    @Nonnull
    public ElasticSinkBuilder<T> bulkRequestFn(@Nonnull SupplierEx<BulkRequest> bulkRequestFn) {
        this.bulkRequestFn = checkNonNullAndSerializable(bulkRequestFn, "bulkRequestFn");
        return this;
    }

    /**
     * Set the function mapping the item from a pipeline item to an index
     * request
     * <p>
     * For example, to create an IndexRequest for a versioned document:
     * <pre>{@code
     * builder.mapToRequestFn((mapItem) ->
     *                      new IndexRequest("my-index")
     *                              .source(map)
     *                              .version((Long) map.get("version"))
     * }</pre>
     *
     * This parameter is required.
     *
     * @param mapToRequestFn maps an item from the stream to an
     *                       {@link org.elasticsearch.action.index.IndexRequest},
     *                       {@link org.elasticsearch.action.update.UpdateRequest}
     *                       or {@link org.elasticsearch.action.delete.DeleteRequest}
     * @param <T_NEW> type of the items from the pipeline
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    public <T_NEW> ElasticSinkBuilder<T_NEW> mapToRequestFn(
            @Nonnull FunctionEx<? super T_NEW, ? extends DocWriteRequest<?>> mapToRequestFn
    ) {
        ElasticSinkBuilder<T_NEW> newThis = (ElasticSinkBuilder<T_NEW>) this;
        newThis.mapToRequestFn = checkNonNullAndSerializable(mapToRequestFn, "mapToRequestFn");
        return newThis;
    }

    /**
     * Set the function that provides {@link RequestOptions}
     * <p>
     * It can either return a constant value or a value based on provided request.
     * <p>
     * For example, to provide a custom authentication header:
     * <pre>{@code
     * sinkBuilder.optionsFn((request) -> {
     *     RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
     *     builder.addHeader("Authorization", "Bearer " + TOKEN);
     *     return builder.build();
     * })
     * }</pre>
     *
     * @param optionsFn function that provides {@link RequestOptions}
     * @see <a
     * href="https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-low-usage-requests.html">
     * RequestOptions in Elastic documentation</a>
     */
    @Nonnull
    public ElasticSinkBuilder<T> optionsFn(@Nonnull FunctionEx<? super ActionRequest, RequestOptions> optionsFn) {
        this.optionsFn = checkNonNullAndSerializable(optionsFn, "optionsFn");
        return this;
    }

    /**
     * Number of retries the connector will do in addition to Elastic client
     * retries
     *
     * Elastic client tries to connect to a node only once for each request.
     * When a request fails the node is marked dead and is not retried again
     * for the request. This causes problems with single node clusters or in a
     * situation where whole cluster becomes unavailable at the same time (e.g.
     * due to a network issue).
     *
     * The initial delay is 2s, increasing by factor of 2 with each retry (4s,
     * 8s, 16s, ..).
     *
     * @param retries number of retries, defaults to 5
     */
    @Nonnull
    public ElasticSinkBuilder<T> retries(int retries) {
        if (retries < 0) {
            throw new IllegalArgumentException("retries must be positive");
        }
        this.retries = retries;
        return this;
    }

    /**
     * Create a sink that writes data into Elasticsearch based on this builder configuration
     */
    @Nonnull
    public Sink<T> build() {
        requireNonNull(clientFn, "clientFn is not set");
        requireNonNull(mapToRequestFn, "mapToRequestFn is not set");

        return SinkBuilder
                .sinkBuilder(DEFAULT_NAME, ctx ->
                        new BulkContext(new RestHighLevelClient(clientFn.get()), bulkRequestFn,
                                optionsFn, retries, ctx.logger()))
                .<T>receiveFn((bulkContext, item) -> bulkContext.add(mapToRequestFn.apply(item)))
                .flushFn(BulkContext::flush)
                .destroyFn(BulkContext::close)
                .preferredLocalParallelism(DEFAULT_LOCAL_PARALLELISM)
                .build();
    }

    static final class BulkContext {

        private final RestHighLevelClient client;
        private final SupplierEx<BulkRequest> bulkRequestSupplier;
        private final FunctionEx<? super ActionRequest, RequestOptions> optionsFn;
        private final int retries;

        private BulkRequest bulkRequest;
        private final ILogger logger;

        BulkContext(
                RestHighLevelClient client, SupplierEx<BulkRequest> bulkRequestSupplier,
                FunctionEx<? super ActionRequest, RequestOptions> optionsFn, int retries, ILogger logger
        ) {
            this.client = client;
            this.bulkRequestSupplier = bulkRequestSupplier;
            this.optionsFn = optionsFn;

            this.bulkRequest = bulkRequestSupplier.get();
            this.retries = retries;
            this.logger = logger;
        }

        void add(DocWriteRequest<?> request) {
            bulkRequest.add(request);
        }

        void flush() throws IOException {
            if (!bulkRequest.requests().isEmpty()) {
                withRetry(
                        () -> {
                            BulkResponse response = client.bulk(bulkRequest, optionsFn.apply(bulkRequest));
                            if (response.hasFailures()) {
                                throw new JetException(response.buildFailureMessage());
                            }
                            if (logger.isFineEnabled()) {
                                logger.fine("BulkRequest with " + bulkRequest.requests().size() + " requests succeeded");
                            }
                            return response;
                        },
                        retries,
                        IOException.class, JetException.class
                );
                bulkRequest = bulkRequestSupplier.get();
            }
        }

        void close() throws IOException {
            logger.fine("Closing BulkContext");
            try {
                flush();
            } finally {
                client.close();
            }
        }
    }

}
