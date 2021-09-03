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

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.elasticsearch.client.RequestOptions.DEFAULT;

public abstract class CommonElasticSinksTest extends BaseElasticTest {

    @Test
    public void given_singleDocument_whenWriteToElasticSink_then_singleDocumentInIndex() throws Exception {
        Sink<TestItem> elasticSink = new ElasticSinkBuilder<>()
                .clientFn(elasticClientSupplier())
                .bulkRequestFn(() -> new BulkRequest().setRefreshPolicy(RefreshPolicy.IMMEDIATE))
                .mapToRequestFn((TestItem item) -> new IndexRequest("my-index", "document").source(item.asMap()))
                .build();

        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(new TestItem("id", "Frantisek")))
         .writeTo(elasticSink);

        submitJob(p);

        assertSingleDocument();
    }

    @Test
    public void given_batchOfDocuments_whenWriteToElasticSink_then_batchOfDocumentsInIndex() throws IOException {
        Sink<TestItem> elasticSink = new ElasticSinkBuilder<>()
                .clientFn(elasticClientSupplier())
                .mapToRequestFn((TestItem item) -> new IndexRequest("my-index", "document").source(item.asMap()))
                .build();

        int batchSize = 10_000;
        TestItem[] items = new TestItem[batchSize];
        for (int i = 0; i < batchSize; i++) {
            items[i] = new TestItem("id" + i, "name" + i);
        }
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(items))
         .writeTo(elasticSink);

        submitJob(p);
        refreshIndex();

        SearchResponse response = elasticClient.search(new SearchRequest("my-index"), DEFAULT);
        long totalHits = response.getHits().getTotalHits();
        assertThat(totalHits).isEqualTo(batchSize);
    }

    @Test
    public void given_sinkCreatedByFactoryMethod_whenWriteToElasticSink_thenDocumentInIndex() throws Exception {
        Sink<TestItem> elasticSink = ElasticSinks.elastic(
                elasticClientSupplier(),
                item -> new IndexRequest("my-index", "document").source(item.asMap())
        );

        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(new TestItem("id", "Frantisek")))
         .writeTo(elasticSink);

        submitJob(p);
        refreshIndex();

        assertSingleDocument();
    }

    @Test
    public void given_documentInIndex_whenWriteToElasticSinkUpdateRequest_then_documentsInIndexUpdated() throws Exception {
        Map<String, Object> doc = new HashMap<>();
        doc.put("name", "Fra");
        String id = indexDocument("my-index", doc);

        Sink<TestItem> elasticSink = ElasticSinks.elastic(
                elasticClientSupplier(),
                item -> new UpdateRequest("my-index", "document", item.getId()).doc(item.asMap())
        );

        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(new TestItem(id, "Frantisek")))
         .writeTo(elasticSink);

        submitJob(p);
        refreshIndex();

        assertSingleDocument(id, "Frantisek");
    }

    @Test
    public void given_documentInIndex_whenWriteToElasticSinkDeleteRequest_then_documentIsDeleted() throws Exception {
        Map<String, Object> doc = new HashMap<>();
        doc.put("name", "Fra");
        String id = indexDocument("my-index", doc);

        Sink<TestItem> elasticSink = ElasticSinks.elastic(
                elasticClientSupplier(),
                (item) -> new DeleteRequest("my-index", "document", item.getId())
        );

        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(new TestItem(id, "Frantisek")))
         .writeTo(elasticSink);

        submitJob(p);
        refreshIndex();

        assertNoDocuments("my-index");
    }

    /**
     * Regression test for checking that behavior was not unexpectedly changed.
     * It is possible that behavior will be changed in any of future version
     * since failing job based on unsuccessful delete/update leads to problems
     * when job are restarted.
     */
    @Test
    public void given_documentNotInIndex_whenWriteToElasticSinkUpdateRequest_then_jobShouldFail() throws Exception {
        elasticClient.indices().create(new CreateIndexRequest("my-index"), RequestOptions.DEFAULT);

        Sink<TestItem> elasticSink = new ElasticSinkBuilder<>()
                .clientFn(elasticClientSupplier())
                .mapToRequestFn((TestItem item)
                        -> new UpdateRequest("my-index", "document", item.getId()).doc(item.asMap()))
                .retries(0)
                .build();

        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(new TestItem("notExist", "Frantisek")))
         .writeTo(elasticSink);

        assertThatThrownBy(() -> submitJob(p))
                .hasRootCauseInstanceOf(JetException.class)
                .hasStackTraceContaining("document missing");
    }

    @Test
    public void given_documentInIndex_whenWriteToElasticSinkDeleteRequestTwice_then_jobShouldFinishSuccessfully()
            throws Exception {

        Map<String, Object> doc = new HashMap<>();
        doc.put("name", "Frantisek");
        String id = indexDocument("my-index", doc);

        Sink<String> elasticSink = new ElasticSinkBuilder<>()
                .clientFn(elasticClientSupplier())
                .mapToRequestFn((String item) -> new DeleteRequest("my-index", "document", item))
                .bulkRequestFn(() -> new BulkRequest().setRefreshPolicy(RefreshPolicy.IMMEDIATE))
                .build();

        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(id))
         .writeTo(elasticSink);

        // Submit job 2x to delete non-existing document on 2nd run
        submitJob(p);
        submitJob(p);

        assertNoDocuments("my-index");
    }

    private void refreshIndex() throws IOException {
        // Need to refresh index because the default bulk request doesn't do it and we may not see the result
        elasticClient.indices().refresh(new RefreshRequest("my-index"), DEFAULT);
    }

    static class TestItem implements Serializable {

        private static final long serialVersionUID = 1L;
        private final String id;
        private final String name;

        TestItem(String id, String name) {
            this.id = id;
            this.name = name;
        }

        public String getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public Map<String, Object> asMap() {
            Map<String, Object> map = new HashMap<>();
            map.put("id", id);
            map.put("name", name);
            return map;
        }

    }
}
