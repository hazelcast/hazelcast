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

package com.hazelcast.jet.elastic.impl;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.logging.ILogger;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.slice.SliceBuilder;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

final class ElasticSourceP<T> extends AbstractProcessor {

    private final ElasticSourceConfiguration<T> configuration;
    private final List<Shard> shards;
    private RestHighLevelClient client;
    private ILogger logger;
    private Traverser<T> traverser;

    // need to keep ElasticScrollTraverser to be able to close the scroll when needed
    private ElasticScrollTraverser scrollTraverser;
    private RestClient restClient;

    ElasticSourceP(ElasticSourceConfiguration<T> configuration, List<Shard> shards) {
        this.configuration = configuration;
        this.shards = shards;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        super.init(context);

        logger = context.logger();
        logger.fine("init");

        client = configuration.clientFn().get();
        SearchRequest sr = configuration.searchRequestFn().get();
        sr.scroll(configuration.scrollKeepAlive());

        if (configuration.isSlicingEnabled()) {
            if (configuration.isCoLocatedReadingEnabled()) {
                int sliceId = context.localProcessorIndex();
                int totalSlices = context.localParallelism();
                if (totalSlices > 1) {
                    logger.fine("Slice id=" + sliceId + ", max=" + totalSlices);
                    sr.source().slice(new SliceBuilder(sliceId, totalSlices));
                }
            } else {
                int sliceId = context.globalProcessorIndex();
                int totalSlices = context.totalParallelism();
                if (totalSlices > 1) {
                    logger.fine("Slice id=" + sliceId + ", max=" + totalSlices);
                    sr.source().slice(new SliceBuilder(sliceId, totalSlices));
                }
            }
        }

        restClient = ElasticCatClient.getRestClient(client);
        if (configuration.isCoLocatedReadingEnabled()) {
            logger.fine("Assigned shards: " + shards);
            if (shards.isEmpty()) {
                traverser = Traversers.empty();
                return;
            }

            HttpHost host = createLocalElasticNode();
            restClient.setHosts(host);
            String preference =
                    "_shards:" + shards.stream().map(shard -> String.valueOf(shard.getShard())).collect(joining(","))
                            + "|_only_local";
            sr.preference(preference);
        }

        scrollTraverser = new ElasticScrollTraverser(configuration, client, sr, logger);
        traverser = scrollTraverser.map(configuration.mapToItemFn());
    }

    private HttpHost createLocalElasticNode() {
        List<String> ips = shards.stream().map(Shard::getHttpAddress).distinct().collect(toList());
        if (ips.size() != 1) {
            throw new JetException("Should receive shards from single local node, got: " + ips);
        }
        String localIp = ips.get(0);
        return HttpHost.create(localIp);
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    public boolean complete() {
        return emitFromTraverser(traverser);
    }

    @Override
    public void close() throws Exception {
        // scrollTraverser is null when scroll init failed
        if (scrollTraverser != null) {
            scrollTraverser.close();
        }

        try {
            restClient.close();
        } catch (Exception e) { // IOException on client.close()
            logger.fine("Could not close client", e);
        }
    }

    static class ElasticScrollTraverser implements Traverser<SearchHit> {

        private final ILogger logger;

        private final RestHighLevelClient client;
        private final String scrollKeepAlive;

        private SearchHits hits;
        private int nextHit;
        private String scrollId;

        ElasticScrollTraverser(ElasticSourceConfiguration<?> configuration, RestHighLevelClient client, SearchRequest sr,
                               ILogger logger) {
            this.client = client;
            this.scrollKeepAlive = configuration.scrollKeepAlive();
            this.logger = logger;

            try {
                SearchResponse response = this.client.search(sr);

                // These should be always present, even when there are no results
                hits = requireNonNull(response.getHits(), "null hits in the response");
                scrollId = response.getScrollId();
                if (scrollId == null && hits.getHits().length > 0) {
                    throw new IllegalStateException("Unexpected response: returned scrollId is null, but hits.length " +
                            "is not zero (" + hits.getHits().length + "). Please file a bug.");
                }

                long totalHits = hits.getTotalHits();
                logger.fine("Initialized scroll with scrollId " + scrollId + ", total results " + ", " + totalHits);
            } catch (IOException e) {
                throw new JetException("Could not execute SearchRequest to Elastic", e);
            }
        }

        @Override
        public SearchHit next() {
            if (hits.getHits().length == 0) {
                scrollId = null;
                return null;
            }

            if (nextHit >= hits.getHits().length) {
                try {
                    SearchScrollRequest ssr = new SearchScrollRequest(scrollId);
                    ssr.scroll(scrollKeepAlive);

                    SearchResponse searchResponse = client.searchScroll(ssr);
                    hits = searchResponse.getHits();
                    if (hits.getHits().length == 0) {
                        return null;
                    }
                    nextHit = 0;
                } catch (IOException e) {
                    throw new JetException("Could not execute SearchScrollRequest to Elastic", e);
                }
            }

            return hits.getAt(nextHit++);
        }

        public void close() {
            if (scrollId != null) {
                clearScroll(scrollId);
                scrollId = null;
            }
        }

        private void clearScroll(String scrollId) {
            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);
            try {
                ClearScrollResponse response = client.clearScroll(clearScrollRequest);

                if (response.isSucceeded()) {
                    logger.fine("Succeeded clearing " + response.getNumFreed() + " scrolls");
                } else {
                    logger.warning("Clearing scroll " + scrollId + " failed");
                }
            } catch (IOException e) {
                logger.fine("Could not clear scroll with scrollId=" + scrollId, e);
            }
        }
    }

}
