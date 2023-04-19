/*
 * Copyright 2023 Hazelcast Inc.
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
import com.hazelcast.jet.Job;
import com.hazelcast.jet.elastic.CommonElasticSinksTest.TestItem;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.test.HazelcastTestSupport;
import org.apache.http.HttpHost;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.ToxiproxyContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.elasticsearch.client.RequestOptions.DEFAULT;

/**
 * Test running single Jet member locally and Elastic in docker
 */
public class RetryElasticSinkTest extends BaseElasticTest {

    @Rule
    public ToxiproxyContainer toxiproxy = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.5.0")
            .withNetwork(ElasticSupport.network)
            .withNetworkAliases("toxiproxy");

    private TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Override
    protected HazelcastInstance createHazelcastInstance() {
        // This starts very quickly, no need to cache the instance
        return factory.newHazelcastInstance(config());
    }

    @Test
    public void when_elasticNotInitiallyAvailable_then_shouldWriteAllDocuments() throws Exception {
        int batchSize = 10_000;
        TestItem[] items = new TestItem[batchSize];
        for (int i = 0; i < batchSize; i++) {
            items[i] = new TestItem("id" + i, "name" + i);
        }

        ToxiproxyContainer.ContainerProxy elasticProxy = toxiproxy.getProxy(ElasticSupport.elastic.get(), 9200);
        try {
            elasticProxy.setConnectionCut(true);
            String address = toxiproxy.getHost();
            HttpHost[] hosts = {new HttpHost(address, elasticProxy.getProxyPort())};
            Job job = submitJobNoWait(
                    retryElasticSinkTestPipeline("my-index", hosts, 5000, items)
            );
            HazelcastTestSupport.sleepSeconds(10);
            elasticProxy.setConnectionCut(false);
            job.join();
            refreshIndex();

            SearchResponse response = elasticClient.search(new SearchRequest("my-index"), DEFAULT);
            TotalHits totalHits = response.getHits().getTotalHits();
            assertThat(totalHits.value).isEqualTo(batchSize);
        } finally {
            // clean up test; we can call setConnectionCut(false) twice
            elasticProxy.setConnectionCut(false);
        }
    }

    public static Pipeline retryElasticSinkTestPipeline(
            String index,
            HttpHost[] hosts,
            int elasticTimeout,
            TestItem... items) {
        Sink<TestItem> elasticSink = new ElasticSinkBuilder<>()
                .clientFn(elasticClientSupplier(hosts, elasticTimeout))
                .bulkRequestFn(() -> new BulkRequest().setRefreshPolicy(RefreshPolicy.IMMEDIATE))
                .mapToRequestFn((TestItem item) -> new IndexRequest(index).source(item.asMap()))
                .build();

        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(items))
         .writeTo(elasticSink);

        return p;
    }

    public static SupplierEx<RestClientBuilder> elasticClientSupplier(HttpHost[] hosts, int elasticTimeout) {
        return () -> RestClient.builder(hosts).setRequestConfigCallback(
                requestConfigBuilder -> requestConfigBuilder
                        .setConnectionRequestTimeout(elasticTimeout)
                        .setConnectTimeout(elasticTimeout)
                        .setSocketTimeout(elasticTimeout));
    }

}
