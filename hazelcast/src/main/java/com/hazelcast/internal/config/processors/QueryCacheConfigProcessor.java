/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.config.processors;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.QueryCacheConfig;

import org.w3c.dom.Node;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.config.DomConfigHelper.childElements;
import static com.hazelcast.internal.config.DomConfigHelper.cleanNodeName;
import static com.hazelcast.internal.config.DomConfigHelper.getBooleanValue;
import static com.hazelcast.internal.config.DomConfigHelper.getIntegerValue;
import static com.hazelcast.internal.config.DomConfigHelper.getTextContent;
import static com.hazelcast.internal.util.StringUtil.upperCaseInternal;

class QueryCacheConfigProcessor implements Processor<QueryCacheConfig> {
    private final Node node;
    private final boolean domLevel3;
    private final Map<String, BiConsumer<QueryCacheConfig, Node>> childNameToConsumer = new HashMap<>();

    QueryCacheConfigProcessor(Node node, boolean domLevel3) {
        this.node = node;
        this.domLevel3 = domLevel3;
        childNameToConsumer.put(
            "entry-listeners",
            (queryCacheConfig, childNode) -> new CollectionProcessor<>(
                childNode,
                "entry-listener",
                listenerNode -> new EntryListenerProcessor(listenerNode, domLevel3),
                queryCacheConfig::addEntryListenerConfig
            ).process()
        );
        childNameToConsumer.put(
            "include-value",
            (queryCacheConfig, childNode) ->
                queryCacheConfig.setIncludeValue(getBooleanValue(nodeValue(domLevel3, childNode)))
        );
        childNameToConsumer.put("batch-size", (queryCacheConfig, childNode) ->
            queryCacheConfig.setBatchSize(getIntegerValue("batch-size", nodeValue(domLevel3, childNode))));
        childNameToConsumer.put("buffer-size", (queryCacheConfig, childNode) ->
            queryCacheConfig.setBufferSize(getIntegerValue("buffer-size", nodeValue(domLevel3, childNode))));
        childNameToConsumer.put("delay-seconds", (queryCacheConfig, childNode) ->
            queryCacheConfig.setDelaySeconds(getIntegerValue("delay-seconds", nodeValue(domLevel3, childNode))));
        childNameToConsumer.put("in-memory-format", (queryCacheConfig, childNode) ->
            queryCacheConfig.setInMemoryFormat(InMemoryFormat.valueOf(upperCaseInternal(nodeValue(domLevel3, childNode)))));
        childNameToConsumer.put("coalesce", (queryCacheConfig, childNode) ->
            queryCacheConfig.setCoalesce(getBooleanValue(nodeValue(domLevel3, childNode))));
        childNameToConsumer.put("populate", (queryCacheConfig, childNode) ->
            queryCacheConfig.setPopulate(getBooleanValue(nodeValue(domLevel3, childNode))));
        childNameToConsumer.put(
            "indexes",
            (queryCacheConfig, childNode) ->
                new CollectionProcessor<>(
                    childNode,
                    "index",
                    indexNode -> new IndexProcessor(indexNode, domLevel3),
                    queryCacheConfig::addIndexConfig
                ).process()
        );
        childNameToConsumer.put("predicate", (queryCacheConfig, childNode) ->
            queryCacheConfig.setPredicateConfig(new PredicateConfigProcessor(childNode, domLevel3).process()));
        childNameToConsumer.put("eviction", (queryCacheConfig, childNode) ->
            queryCacheConfig.setEvictionConfig(new QueryCacheEvictionConfigProcessor(childNode, domLevel3).process()));
    }

    private String nodeValue(boolean domLevel3, Node node) {
        return getTextContent(node, domLevel3).trim();
    }

    @Override
    public QueryCacheConfig process() {
        QueryCacheConfig queryCacheConfig = new QueryCacheConfig(cacheName());
        for (Node child : childElements(node)) {
            childNameToConsumer.get(cleanNodeName(child)).accept(queryCacheConfig, child);
        }
        return queryCacheConfig;
    }

    private String cacheName() {
        return getTextContent(node.getAttributes().getNamedItem("name"), domLevel3);
    }
}
