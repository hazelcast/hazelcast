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

package com.hazelcast.jet.elastic.impl;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;

import javax.annotation.Nonnull;
import java.io.Serializable;

/**
 * Configuration for the Elastic source which is Serializable
 *
 * Avoids passing multiple parameters via constructors
 * Builder -> ElasticSourcePMetaSupplier -> ElasticSourcePSupplier
 * -> ElasticSourceP -> ElasticScrollTraverser
 */
public class ElasticSourceConfiguration<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private final SupplierEx<RestHighLevelClient> clientFn;
    private final SupplierEx<SearchRequest> searchRequestFn;
    private final FunctionEx<? super ActionRequest, RequestOptions> optionsFn;
    private final FunctionEx<? super SearchHit, T> mapToItemFn;
    private final boolean slicing;
    private final boolean coLocatedReading;
    private final String scrollKeepAlive;
    private final int retries;

    public ElasticSourceConfiguration(
            SupplierEx<RestHighLevelClient> clientFn,
            SupplierEx<SearchRequest> searchRequestFn,
            FunctionEx<? super ActionRequest, RequestOptions> optionsFn,
            FunctionEx<? super SearchHit, T> mapToItemFn,
            boolean slicing, boolean coLocatedReading,
            String scrollKeepAlive, int retries
    ) {
        this.clientFn = clientFn;
        this.searchRequestFn = searchRequestFn;
        this.optionsFn = optionsFn;
        this.mapToItemFn = mapToItemFn;
        this.slicing = slicing;
        this.coLocatedReading = coLocatedReading;
        this.scrollKeepAlive = scrollKeepAlive;
        this.retries = retries;
    }

    @Nonnull
    public SupplierEx<RestHighLevelClient> clientFn() {
        return clientFn;
    }

    @Nonnull
    public SupplierEx<SearchRequest> searchRequestFn() {
        return searchRequestFn;
    }

    @Nonnull
    public FunctionEx<? super SearchHit, T> mapToItemFn() {
        return mapToItemFn;
    }

    public FunctionEx<? super ActionRequest, RequestOptions> optionsFn() {
        return optionsFn;
    }

    public boolean isSlicingEnabled() {
        return slicing;
    }

    public boolean isCoLocatedReadingEnabled() {
        return coLocatedReading;
    }

    public String scrollKeepAlive() {
        return scrollKeepAlive;
    }

    public int retries() {
        return retries;
    }

}
