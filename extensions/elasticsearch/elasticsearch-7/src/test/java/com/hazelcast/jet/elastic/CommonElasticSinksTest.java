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
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.jet.elastic.pipeline.CommonElasticSinksPipeline.deleteItemsFromIndexPipeline;
import static com.hazelcast.jet.elastic.pipeline.CommonElasticSinksPipeline.updateItemsInIndexPipeline;
import static com.hazelcast.jet.elastic.pipeline.CommonElasticSinksPipeline.writeItemsToIndexPipeline;
import static com.hazelcast.jet.elastic.pipeline.CommonElasticSinksPipeline.writeItemsToIndexUsingSourceFactoryMethodPipeline;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.elasticsearch.client.RequestOptions.DEFAULT;

public abstract class CommonElasticSinksTest extends BaseElasticTest {

    @Test
    public void given_singleDocument_whenWriteToElasticSink_then_singleDocumentInIndex() throws Exception {
        Pipeline p = writeItemsToIndexPipeline(
                "my-index",
                elasticPipelineClientSupplier(),
                new TestItem("id", "Frantisek")
        );

        submitJob(p);
        assertSingleDocument();
    }

    @Test
    public void given_batchOfDocuments_whenWriteToElasticSink_then_batchOfDocumentsInIndex() throws IOException {
        int batchSize = 10_000;
        TestItem[] items = new TestItem[batchSize];
        for (int i = 0; i < batchSize; i++) {
            items[i] = new TestItem("id" + i, "name" + i);
        }

        Pipeline p = writeItemsToIndexPipeline("my-index", elasticPipelineClientSupplier(), items);
        submitJob(p);
        refreshIndex();

        SearchResponse response = elasticClient.search(new SearchRequest("my-index"), DEFAULT);
        TotalHits totalHits = response.getHits().getTotalHits();
        assertThat(totalHits.value).isEqualTo(batchSize);
    }

    @Test
    public void given_sinkCreatedByFactoryMethod_whenWriteToElasticSink_thenDocumentInIndex() throws Exception {
        Pipeline p = writeItemsToIndexUsingSourceFactoryMethodPipeline(
                "my-index",
                elasticPipelineClientSupplier(),
                new TestItem("id", "Frantisek")
        );

        submitJob(p);
        refreshIndex();

        assertSingleDocument();
    }

    @Test
    public void given_documentInIndex_whenWriteToElasticSinkUpdateRequest_then_documentsInIndexUpdated() throws Exception {
        Map<String, Object> doc = new HashMap<>();
        doc.put("name", "Fra");
        String id = indexDocument("my-index", doc);

        Pipeline p = updateItemsInIndexPipeline(
                "my-index",
                elasticPipelineClientSupplier(),
                new TestItem(id, "Frantisek")
        );
        submitJob(p);
        refreshIndex();

        assertSingleDocument(id, "Frantisek");
    }

    @Test
    public void given_documentInIndex_whenWriteToElasticSinkDeleteRequest_then_documentIsDeleted() throws Exception {
        Map<String, Object> doc = new HashMap<>();
        doc.put("name", "Fra");
        String id = indexDocument("my-index", doc);

        Pipeline p = deleteItemsFromIndexPipeline(
                "my-index",
                elasticPipelineClientSupplier(),
                new TestItem(id, "Frantisek")
        );

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

        Pipeline p = updateItemsInIndexPipeline(
                "my-index",
                elasticPipelineClientSupplier(),
                new TestItem("notExist", "Frantisek")
        );

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

        Pipeline p = deleteItemsFromIndexPipeline(
                "my-index",
                elasticPipelineClientSupplier(),
                new TestItem(id, "Frantisek")
        );

        // Submit job 2x to delete non-existing document on 2nd run
        submitJob(p);
        submitJob(p);

        assertNoDocuments("my-index");
    }

    private void refreshIndex() throws IOException {
        // Need to refresh index because the default bulk request doesn't do it and we may not see the result
        elasticClient.indices().refresh(new RefreshRequest("my-index"), DEFAULT);
    }

    public static class TestItem implements Serializable {

        private static final long serialVersionUID = 1L;
        private final String id;
        private final String name;

        public TestItem(String id, String name) {
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
