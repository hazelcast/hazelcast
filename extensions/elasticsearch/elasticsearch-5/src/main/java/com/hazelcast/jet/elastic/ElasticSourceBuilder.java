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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.elastic.impl.ElasticSourceConfiguration;
import com.hazelcast.jet.elastic.impl.ElasticSourcePMetaSupplier;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Sources;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.impl.util.Util.checkNonNullAndSerializable;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static java.util.Objects.requireNonNull;

/**
 * Builder for Elasticsearch source which reads data from Elasticsearch and
 * converts SearchHits using provided {@code mapToItemFn}
 * <p>
 * Usage:
 * <pre>{@code
 * BatchSource<String> source = new ElasticSourceBuilder<String>()
 *   .clientFn(() -> client(host, port))
 *   .searchRequestFn(() -> new SearchRequest("my-index"))
 *   .mapToItemFn(SearchHit::getSourceAsString)
 *   .build();
 *
 * BatchStage<String> stage = p.readFrom(source);
 * }</pre>
 *
 * Requires {@link #clientFn(SupplierEx)},
 * {@link #searchRequestFn(SupplierEx)} and {@link #mapToItemFn(FunctionEx)}.
 *
 * @param <T> type of the output of the mapping function from {@link SearchHit} -> T
 * @since 4.2
 */
public final class ElasticSourceBuilder<T> {

    private static final String DEFAULT_NAME = "elasticSource";

    private SupplierEx<RestClientBuilder> clientFn;
    private SupplierEx<SearchRequest> searchRequestFn;
    private FunctionEx<? super SearchHit, T> mapToItemFn;
    private boolean slicing;
    private boolean coLocatedReading;
    private String scrollKeepAlive = "1m"; // Using String because it needs to be Serializable

    /**
     * Build Elasticsearch {@link BatchSource} with supplied parameters
     *
     * @return configured source which is to be used in the pipeline
     */
    @Nonnull
    public BatchSource<T> build() {
        requireNonNull(clientFn, "clientFn must be set");
        requireNonNull(searchRequestFn, "searchRequestFn must be set");
        requireNonNull(mapToItemFn, "mapToItemFn must be set");

        ElasticSourceConfiguration<T> configuration = new ElasticSourceConfiguration<>(
                restHighLevelClientFn(clientFn),
                searchRequestFn, mapToItemFn, slicing, coLocatedReading,
                scrollKeepAlive
        );
        ElasticSourcePMetaSupplier<T> metaSupplier = new ElasticSourcePMetaSupplier<>(configuration);
        return Sources.batchFromProcessor(DEFAULT_NAME, metaSupplier);
    }

    // Don't inline - it would capture this.clientFn and would need to serialize whole builder instance
    private SupplierEx<RestHighLevelClient> restHighLevelClientFn(SupplierEx<RestClientBuilder> clientFn) {
        return () -> new RestHighLevelClient(clientFn.get().build());
    }

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
    public ElasticSourceBuilder<T> clientFn(@Nonnull SupplierEx<RestClientBuilder> clientFn) {
        this.clientFn = checkNonNullAndSerializable(clientFn, "clientFn");
        return this;
    }

    /**
     * Set the search request supplier function
     * <p>
     * The connector executes this search request to retrieve documents
     * from Elasticsearch.
     * <p>
     * For example, to create SearchRequest limited to an index `logs`:
     * <pre>{@code
     * builder.searchRequestFn(() -> new SearchRequest("logs"))
     * }</pre>
     *
     * This parameter is required.
     *
     * @param searchRequestFn search request supplier function
     */
    @Nonnull
    public ElasticSourceBuilder<T> searchRequestFn(@Nonnull SupplierEx<SearchRequest> searchRequestFn) {
        this.searchRequestFn = checkSerializable(searchRequestFn, "searchRequestFn");
        return this;
    }

    /**
     * Set the function to map SearchHit to a pipeline item
     * <p>
     * For example, to map a SearchHit to a value of a field `productId`:
     * <pre>{@code
     * builder.mapToItemFn(hit -> (String) hit.getSourceAsMap().get("productId"))
     * }</pre>
     *
     * This parameter is required.
     *
     * @param mapToItemFn maps search hits to output items
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    public <T_NEW> ElasticSourceBuilder<T_NEW> mapToItemFn(@Nonnull FunctionEx<? super SearchHit, T_NEW> mapToItemFn) {
        ElasticSourceBuilder<T_NEW> newThis = (ElasticSourceBuilder<T_NEW>) this;
        newThis.mapToItemFn = checkSerializable(mapToItemFn, "mapToItemFn");
        return newThis;
    }

    /**
     * Enable slicing
     * <p>
     * Number of slices is equal to {@code globalParallelism
     * (localParallelism * numberOfNodes)} when only slicing is enabled. When
     * co-located reading is enabled as well then number of slices for
     * particular node is equal to {@code localParallelism}.
     * <p>
     * Use this option to read from multiple shards in parallel. It can
     * also be used on single shard, but it may increase initial latency.
     * See Elastic documentation for
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-body.html#sliced-scroll">
     *     Sliced Scroll</a> for details.
     */
    @Nonnull
    public ElasticSourceBuilder<T> enableSlicing() {
        this.slicing = true;
        return this;
    }

    /**
     * Enable co-located reading
     *
     * Jet cluster member must run exactly on the same nodes as Elastic cluster.
     */
    @Nonnull
    public ElasticSourceBuilder<T> enableCoLocatedReading() {
        this.coLocatedReading = true;
        return this;
    }

    /**
     * Set the keepAlive for Elastic search scroll
     * <p>
     * The value must be in Elastic time unit format, e.g. 500ms for 500 milliseconds, 30s for 30 seconds,
     * 5m for 5 minutes. See {@link SearchRequest#scroll(String)}.
     *
     * @param scrollKeepAlive keepAlive value, this must be high enough to
     *                        process all results from a single scroll, default
     *                        value 1m
     */
    @Nonnull
    public ElasticSourceBuilder<T> scrollKeepAlive(@Nonnull String scrollKeepAlive) {
        this.scrollKeepAlive = requireNonNull(scrollKeepAlive, scrollKeepAlive);
        return this;
    }

}
