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
package com.hazelcast.jet.elastic.pipeline;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.elastic.CommonElasticSinksTest;
import com.hazelcast.jet.elastic.ElasticSinkBuilder;
import com.hazelcast.jet.elastic.ElasticSinks;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestClientBuilder;

public final class CommonElasticSinksPipeline {

    private CommonElasticSinksPipeline() {
    }

    public static Pipeline given_singleDocument_whenWriteToElasticSink_then_singleDocumentInIndex_pipeline(
            SupplierEx<RestClientBuilder> elasticSupplier) {
        Pipeline p = Pipeline.create();

        Sink<CommonElasticSinksTest.TestItem> elasticSink = new ElasticSinkBuilder<>()
                .clientFn(elasticSupplier)
                .bulkRequestFn(() -> new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE))
                .mapToRequestFn(
                        (CommonElasticSinksTest.TestItem item) -> new IndexRequest("my-index").source(item.asMap()))
                .build();

        p.readFrom(TestSources.items(new CommonElasticSinksTest.TestItem("id", "Frantisek")))
                .writeTo(elasticSink);

        return p;
    }

    public static Pipeline given_batchOfDocuments_whenWriteToElasticSink_then_batchOfDocumentsInIndex_pipeline(
            SupplierEx<RestClientBuilder> elasticSupplier,
            CommonElasticSinksTest.TestItem[] items) {
        Pipeline p = Pipeline.create();

        Sink<CommonElasticSinksTest.TestItem> elasticSink = new ElasticSinkBuilder<>()
                .clientFn(elasticSupplier)
                .mapToRequestFn(
                        (CommonElasticSinksTest.TestItem item) -> new IndexRequest("my-index").source(item.asMap()))
                .build();

        p.readFrom(TestSources.items(items))
                .writeTo(elasticSink);

        return p;
    }

    public static Pipeline given_sinkCreatedByFactoryMethod_whenWriteToElasticSink_thenDocumentInIndex_pipeline(
            SupplierEx<RestClientBuilder> elasticSupplier) {
        Pipeline p = Pipeline.create();

        Sink<CommonElasticSinksTest.TestItem> elasticSink = ElasticSinks.elastic(
                elasticSupplier,
                item -> new IndexRequest("my-index").source(item.asMap())
        );

        p.readFrom(TestSources.items(new CommonElasticSinksTest.TestItem("id", "Frantisek")))
                .writeTo(elasticSink);

        return p;
    }

    public static Pipeline given_documentInIndex_whenWriteToElasticSinkUpdateRequest_then_documentsInIndexUpdated_pipeline(
            SupplierEx<RestClientBuilder> elasticSupplier,
            String id) {
        Pipeline p = Pipeline.create();

        Sink<CommonElasticSinksTest.TestItem> elasticSink = ElasticSinks.elastic(
                elasticSupplier,
                item -> new UpdateRequest("my-index", item.getId()).doc(item.asMap())
        );

        p.readFrom(TestSources.items(new CommonElasticSinksTest.TestItem(id, "Frantisek")))
                .writeTo(elasticSink);

        return p;
    }

    public static Pipeline given_documentInIndex_whenWriteToElasticSinkDeleteRequest_then_documentIsDeleted_pipeline(
            SupplierEx<RestClientBuilder> elasticSupplier,
            String id) {
        Pipeline p = Pipeline.create();

        Sink<CommonElasticSinksTest.TestItem> elasticSink = ElasticSinks.elastic(
                elasticSupplier,
                (item) -> new DeleteRequest("my-index", item.getId())
        );

        p.readFrom(TestSources.items(new CommonElasticSinksTest.TestItem(id, "Frantisek")))
                .writeTo(elasticSink);

        return p;
    }

    public static Pipeline given_documentNotInIndex_whenWriteToElasticSinkUpdateRequest_then_jobShouldFail_pipeline(
            SupplierEx<RestClientBuilder> elasticSupplier) {
        Pipeline p = Pipeline.create();

        Sink<CommonElasticSinksTest.TestItem> elasticSink = new ElasticSinkBuilder<>()
                .clientFn(elasticSupplier)
                .mapToRequestFn((CommonElasticSinksTest.TestItem item)
                        -> new UpdateRequest("my-index", item.getId()).doc(item.asMap()))
                .retries(0)
                .build();

        p.readFrom(TestSources.items(new CommonElasticSinksTest.TestItem("notExist", "Frantisek")))
                .writeTo(elasticSink);

        return p;
    }

    public static Pipeline documentInIndex_writeToElasticSinkDeleteRequestTwice_jobShouldFinishSuccessfully_pipeline(
            SupplierEx<RestClientBuilder> elasticSupplier,
            String id) {
        Pipeline p = Pipeline.create();

        Sink<String> elasticSink = new ElasticSinkBuilder<>()
                .clientFn(elasticSupplier)
                .mapToRequestFn((String item) -> new DeleteRequest("my-index", item))
                .bulkRequestFn(() -> new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE))
                .build();

        p.readFrom(TestSources.items(id))
                .writeTo(elasticSink);

        return p;
    }

}
