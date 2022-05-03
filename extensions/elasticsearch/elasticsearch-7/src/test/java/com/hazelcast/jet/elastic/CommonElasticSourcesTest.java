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

import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;

import java.io.IOException;

import static com.google.common.collect.ImmutableMap.of;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

/**
 * Base class for Elasticsearch source tests
 * <p>
 * This class is to be extended for each type of environment to run on, e.g.
 * - simple 1 node Jet & Elastic instances
 * - co-located clusters of Jet and Elastic
 * - non co-located clusters of Jet and Elastic
 * <p>
 * Subclasses may add tests specific for particular type of environment.
 * <p>
 * RestHighLevelClient is used to create data in Elastic to isolate possible Source and Sink issues.
 */
public abstract class CommonElasticSourcesTest extends BaseElasticTest {

    @Test
    public void given_emptyIndex_when_readFromElasticSource_then_finishWithNoResults() throws IOException {
        // Ideally we would just create the index but it gives "field _id not found" when there are no documents
        // in the index, not sure if it is an Elastic bug or wrong setup
        //
        // elasticClient.indices().create(new CreateIndexRequest("my-index"), DEFAULT);

        // Instead we index a document and delete it, ending up with index with correct settings applied
        indexDocument("my-index", of("name", "Frantisek"));
        deleteDocuments();

        Pipeline p = given_emptyIndex_when_readFromElasticSource_then_finishWithNoResults_pipeline();
        submitJob(p);

        assertThat(results).isEmpty();
    }

    protected Pipeline given_emptyIndex_when_readFromElasticSource_then_finishWithNoResults_pipeline() {
        Pipeline p = Pipeline.create();

        BatchSource<String> source = new ElasticSourceBuilder<>()
                .clientFn(elasticClientSupplier())
                .searchRequestFn(() -> new SearchRequest("my-index"))
                .mapToItemFn(SearchHit::getSourceAsString)
                .build();

        p.readFrom(source)
                .writeTo(Sinks.list(results));

        return p;
    }

    @Test
    public void given_indexWithOneDocument_whenReadFromElasticSource_thenFinishWithOneResult() {
        indexDocument("my-index", of("name", "Frantisek"));

        Pipeline p = given_indexWithOneDocument_whenReadFromElasticSource_thenFinishWithOneResult_pipeline();
        submitJob(p);
        assertThat(results).containsExactly("Frantisek");
    }

    protected Pipeline given_indexWithOneDocument_whenReadFromElasticSource_thenFinishWithOneResult_pipeline() {
        Pipeline p = Pipeline.create();

        BatchSource<String> source = new ElasticSourceBuilder<>()
                .clientFn(elasticClientSupplier())
                .searchRequestFn(() -> new SearchRequest("my-index"))
                .mapToItemFn(hit -> (String) hit.getSourceAsMap().get("name"))
                .build();

        p.readFrom(source)
                .writeTo(Sinks.list(results));

        return p;
    }

    @Test
    public void given_sourceCreatedByFactoryMethod2_whenReadFromElasticSource_thenFinishWithOneResult() {
        indexDocument("my-index", of("name", "Frantisek"));

        Pipeline p = given_sourceCreatedByFactoryMethod2_whenReadFromElasticSource_thenFinishWithOneResult_pipeline();

        submitJob(p);
        assertThat(results).containsExactly("Frantisek");
    }

    protected Pipeline given_sourceCreatedByFactoryMethod2_whenReadFromElasticSource_thenFinishWithOneResult_pipeline() {
        Pipeline p = Pipeline.create();

        BatchSource<String> source = ElasticSources.elastic(
                elasticClientSupplier(),
                hit -> (String) hit.getSourceAsMap().get("name")
        );

        p.readFrom(source)
                .writeTo(Sinks.list(results));

        return p;
    }

    @Test
    public void given_sourceCreatedByFactoryMethod3_whenReadFromElasticSource_thenFinishWithOneResult() {
        indexDocument("my-index-1", of("name", "Frantisek"));
        indexDocument("my-index-2", of("name", "Vladimir"));

        Pipeline p = given_sourceCreatedByFactoryMethod3_whenReadFromElasticSource_thenFinishWithOneResult_pipeline();

        submitJob(p);
        assertThat(results).containsExactly("Frantisek");
    }

    protected Pipeline given_sourceCreatedByFactoryMethod3_whenReadFromElasticSource_thenFinishWithOneResult_pipeline() {
        Pipeline p = Pipeline.create();

        BatchSource<String> source = ElasticSources.elastic(
                elasticClientSupplier(),
                () -> new SearchRequest("my-index-1"),
                hit -> (String) hit.getSourceAsMap().get("name")
        );

        p.readFrom(source)
                .writeTo(Sinks.list(results));

        return p;
    }

    @Test
    public void given_multipleDocuments_when_readFromElasticSourceWithScroll_then_resultHasAllDocuments()
            throws IOException {

        indexBatchOfDocuments("my-index");

        Pipeline p = multipleDocuments_readFromElasticSourceWithScroll_resultHasAllDocuments_pipeline();

        submitJob(p);
        assertThat(results).hasSize(BATCH_SIZE);
    }

    protected Pipeline multipleDocuments_readFromElasticSourceWithScroll_resultHasAllDocuments_pipeline() {
        Pipeline p = Pipeline.create();

        BatchSource<String> source = new ElasticSourceBuilder<>()
                .clientFn(elasticClientSupplier())
                .searchRequestFn(() -> {
                    SearchRequest sr = new SearchRequest("my-index");

                    sr.source().size(10) // needs to scroll 5 times
                            .query(matchAllQuery());
                    return sr;
                })
                .mapToItemFn(SearchHit::getSourceAsString)
                .build();

        p.readFrom(source)
                .writeTo(Sinks.list(results));

        return p;
    }

    @Test
    public void given_multipleIndexes_when_readFromElasticSourceWithIndexWildcard_then_resultDocumentsFromAllIndexes() {
        indexDocument("my-index-1", of("name", "Frantisek"));
        indexDocument("my-index-2", of("name", "Vladimir"));

        Pipeline p = multipleIndexes_readFromElasticSourceWithIndexWildcard_resultDocumentsFromAllIndexes_pipeline();

        submitJob(p);
        assertThat(results).containsOnlyOnce("Frantisek", "Vladimir");
    }

    protected Pipeline multipleIndexes_readFromElasticSourceWithIndexWildcard_resultDocumentsFromAllIndexes_pipeline() {
        Pipeline p = Pipeline.create();

        BatchSource<String> source = new ElasticSourceBuilder<>()
                .clientFn(elasticClientSupplier())
                .searchRequestFn(() -> new SearchRequest("my-index-*"))
                .mapToItemFn(hit -> (String) hit.getSourceAsMap().get("name"))
                .build();

        p.readFrom(source)
                .writeTo(Sinks.list(results));

        return p;
    }

    @Test
    public void given_multipleIndexes_when_readFromElasticSourceWithIndex_then_resultHasNoDocumentFromOtherIndex() {
        indexDocument("my-index-1", of("name", "Frantisek"));
        indexDocument("my-index-2", of("name", "Vladimir"));

        Pipeline p = multipleIndexes_readFromElasticSourceWithIndex_resultHasNoDocumentFromOtherIndex_pipeline();

        submitJob(p);
        assertThat(results).containsOnlyOnce("Frantisek");
    }

    protected Pipeline multipleIndexes_readFromElasticSourceWithIndex_resultHasNoDocumentFromOtherIndex_pipeline() {
        Pipeline p = Pipeline.create();

        BatchSource<String> source = new ElasticSourceBuilder<>()
                .clientFn(elasticClientSupplier())
                .searchRequestFn(() -> new SearchRequest("my-index-1"))
                .mapToItemFn(hit -> (String) hit.getSourceAsMap().get("name"))
                .build();

        p.readFrom(source)
                .writeTo(Sinks.list(results));

        return p;
    }

    @Test
    public void given_documents_when_readFromElasticSourceWithQuery_then_resultHasMatchingDocuments() {
        indexDocument("my-index", of("name", "Frantisek"));
        indexDocument("my-index", of("name", "Vladimir"));

        Pipeline p = given_documents_when_readFromElasticSourceWithQuery_then_resultHasMatchingDocuments_pipeline();

        submitJob(p);
        assertThat(results).containsOnlyOnce("Frantisek");
    }

    protected Pipeline given_documents_when_readFromElasticSourceWithQuery_then_resultHasMatchingDocuments_pipeline() {
        Pipeline p = Pipeline.create();

        BatchSource<String> source = new ElasticSourceBuilder<>()
                .clientFn(elasticClientSupplier())
                .searchRequestFn(() -> new SearchRequest("my-index")
                .source(new SearchSourceBuilder().query(QueryBuilders.matchQuery("name", "Frantisek"))))
                .mapToItemFn(hit -> (String) hit.getSourceAsMap().get("name"))
                .build();

        p.readFrom(source)
                .writeTo(Sinks.list(results));

        return p;
    }

    @Test
    public void given_documents_whenReadFromElasticSourceWithSlicing_then_resultHasAllDocuments() throws IOException {
        initShardedIndex("my-index");

        Pipeline p = given_documents_whenReadFromElasticSourceWithSlicing_then_resultHasAllDocuments_pipeline();

        submitJob(p);
        assertThat(results).hasSize(BATCH_SIZE);
    }

    protected Pipeline given_documents_whenReadFromElasticSourceWithSlicing_then_resultHasAllDocuments_pipeline() {
        Pipeline p = Pipeline.create();

        BatchSource<String> source = new ElasticSourceBuilder<>()
                .clientFn(elasticClientSupplier())
                .searchRequestFn(() -> new SearchRequest("my-index"))
                .mapToItemFn(SearchHit::getSourceAsString)
                .enableSlicing()
                .build();

        p.readFrom(source)
                .writeTo(Sinks.list(results));

        return p;
    }

    @Test
    public void given_documentsInMultipleIndexes_whenReadFromElasticSourceWithSlicing_then_resultHasAllDocuments()
            throws IOException {

        initShardedIndex("my-index-1");
        initShardedIndex("my-index-2");

        Pipeline p = documentsInMultipleIndexes_readFromElasticSourceWithSlicing_resultHasAllDocuments_pipeline();

        submitJob(p);
        assertThat(results).hasSize(2 * BATCH_SIZE);
    }

    protected Pipeline documentsInMultipleIndexes_readFromElasticSourceWithSlicing_resultHasAllDocuments_pipeline() {
        Pipeline p = Pipeline.create();

        BatchSource<String> source = new ElasticSourceBuilder<>()
                .clientFn(elasticClientSupplier())
                .searchRequestFn(() -> new SearchRequest("my-index-*"))
                .mapToItemFn(SearchHit::getSourceAsString)
                .enableSlicing()
                .build();

        p.readFrom(source)
                .writeTo(Sinks.list(results));

        return p;
    }

    @Test
    public void given_nonExistingIndex_whenReadFromElasticSource_thenThrowException() {
        Pipeline p = given_nonExistingIndex_whenReadFromElasticSource_thenThrowException_pipeline();

        assertThatThrownBy(() -> submitJob(p))
                .hasRootCauseInstanceOf(ResponseException.class)
                .hasStackTraceContaining("no such index [non-existing-index]");
    }

    protected Pipeline given_nonExistingIndex_whenReadFromElasticSource_thenThrowException_pipeline() {
        Pipeline p = Pipeline.create();

        BatchSource<String> source = new ElasticSourceBuilder<>()
                .clientFn(elasticClientSupplier())
                .searchRequestFn(() -> new SearchRequest("non-existing-index"))
                .mapToItemFn(SearchHit::getSourceAsString)
                .retries(0) // we expect the exception -> faster test
                .build();

        p.readFrom(source)
                .writeTo(Sinks.list(results));

        return p;
    }

    @Test
    public void given_aliasMatchingNoIndex_whenReadFromElasticSource_thenReturnNoResults() {
        Pipeline p = given_aliasMatchingNoIndex_whenReadFromElasticSource_thenReturnNoResults_pipeline();

        submitJob(p);
        assertThat(results).isEmpty();
    }

    protected Pipeline given_aliasMatchingNoIndex_whenReadFromElasticSource_thenReturnNoResults_pipeline() {
        Pipeline p = Pipeline.create();

        BatchSource<String> source = new ElasticSourceBuilder<>()
                .clientFn(elasticClientSupplier())
                .searchRequestFn(() -> new SearchRequest("my-index-*"))
                .mapToItemFn(SearchHit::getSourceAsString)
                .build();

        p.readFrom(source)
                .writeTo(Sinks.list(results));

        return p;
    }
}
