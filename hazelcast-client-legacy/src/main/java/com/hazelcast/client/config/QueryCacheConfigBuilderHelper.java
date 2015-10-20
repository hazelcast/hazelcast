/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.config;

import com.hazelcast.config.AbstractXmlConfigHelper;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.PredicateConfig;
import com.hazelcast.config.QueryCacheConfig;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import static com.hazelcast.util.StringUtil.upperCaseInternal;

/**
 * Helper which is used for building {@link QueryCacheConfig} from declarative configurations.
 */
final class QueryCacheConfigBuilderHelper extends AbstractXmlConfigHelper {

    QueryCacheConfigBuilderHelper() {
    }

    void handleQueryCache(ClientConfig clientConfig, Node node) {
        for (org.w3c.dom.Node queryCacheNode : new AbstractXmlConfigHelper.IterableNodeList(node.getChildNodes())) {
            if ("query-cache".equals(cleanNodeName(queryCacheNode))) {
                NamedNodeMap attrs = queryCacheNode.getAttributes();
                String cacheName = getTextContent(attrs.getNamedItem("name"));
                String mapName = getTextContent(attrs.getNamedItem("mapName"));
                QueryCacheConfig queryCacheConfig = new QueryCacheConfig(cacheName);
                for (org.w3c.dom.Node childNode : new AbstractXmlConfigHelper.IterableNodeList(queryCacheNode.getChildNodes())) {
                    String textContent = getTextContent(childNode);
                    String nodeName = cleanNodeName(childNode);
                    populateQueryCacheConfig(queryCacheConfig, childNode, textContent, nodeName);
                }
                clientConfig.addQueryCacheConfig(mapName, queryCacheConfig);
            }
        }
    }

    private void populateQueryCacheConfig(QueryCacheConfig queryCacheConfig,
                                          Node childNode, String textContent, String nodeName) {
        if ("entry-listeners".equals(nodeName)) {
            handleEntryListeners(queryCacheConfig, childNode);
        } else if ("include-value".equals(nodeName)) {
            boolean includeValue = checkTrue(textContent);
            queryCacheConfig.setIncludeValue(includeValue);
        } else if ("batch-size".equals(nodeName)) {
            int batchSize = getIntegerValue("batch-size", textContent.trim(),
                    QueryCacheConfig.DEFAULT_BATCH_SIZE);
            queryCacheConfig.setBatchSize(batchSize);
        } else if ("buffer-size".equals(nodeName)) {
            int bufferSize = getIntegerValue("buffer-size", textContent.trim(),
                    QueryCacheConfig.DEFAULT_BUFFER_SIZE);
            queryCacheConfig.setBufferSize(bufferSize);
        } else if ("delay-seconds".equals(nodeName)) {
            int delaySeconds = getIntegerValue("delay-seconds", textContent.trim(),
                    QueryCacheConfig.DEFAULT_DELAY_SECONDS);
            queryCacheConfig.setDelaySeconds(delaySeconds);
        } else if ("in-memory-format".equals(nodeName)) {
            String value = textContent.trim();
            queryCacheConfig.setInMemoryFormat(InMemoryFormat.valueOf(upperCaseInternal(value)));
        } else if ("coalesce".equals(nodeName)) {
            boolean coalesce = checkTrue(textContent);
            queryCacheConfig.setCoalesce(coalesce);
        } else if ("populate".equals(nodeName)) {
            boolean populate = checkTrue(textContent);
            queryCacheConfig.setPopulate(populate);
        } else if ("indexes".equals(nodeName)) {
            queryCacheIndexesHandle(childNode, queryCacheConfig);
        } else if ("predicate".equals(nodeName)) {
            queryCachePredicateHandler(childNode, queryCacheConfig);
        } else if ("eviction".equals(nodeName)) {
            queryCacheConfig.setEvictionConfig(getEvictionConfig(childNode));
        }
    }

    private EvictionConfig getEvictionConfig(final org.w3c.dom.Node node) {
        final EvictionConfig evictionConfig = new EvictionConfig();
        final Node size = node.getAttributes().getNamedItem("size");
        final Node maxSizePolicy = node.getAttributes().getNamedItem("max-size-policy");
        final Node evictionPolicy = node.getAttributes().getNamedItem("eviction-policy");
        if (size != null) {
            evictionConfig.setSize(Integer.parseInt(getTextContent(size)));
        }
        if (maxSizePolicy != null) {
            evictionConfig.setMaximumSizePolicy(
                    EvictionConfig.MaxSizePolicy.valueOf(
                            upperCaseInternal(getTextContent(maxSizePolicy)))
            );
        }
        if (evictionPolicy != null) {
            evictionConfig.setEvictionPolicy(
                    EvictionPolicy.valueOf(
                            upperCaseInternal(getTextContent(evictionPolicy)))
            );
        }
        return evictionConfig;
    }

    private void handleEntryListeners(QueryCacheConfig queryCacheConfig, Node childNode) {
        for (Node listenerNode : new IterableNodeList(childNode.getChildNodes())) {
            if ("entry-listener".equals(cleanNodeName(listenerNode))) {
                NamedNodeMap listenerNodeAttributes = listenerNode.getAttributes();
                boolean incValue
                        = checkTrue(getTextContent(listenerNodeAttributes.getNamedItem("include-value")));
                boolean local
                        = checkTrue(getTextContent(listenerNodeAttributes.getNamedItem("local")));
                String listenerClass = getTextContent(listenerNode);
                queryCacheConfig.addEntryListenerConfig(
                        new EntryListenerConfig(listenerClass, local, incValue));
            }
        }
    }

    private void queryCachePredicateHandler(Node childNode, QueryCacheConfig queryCacheConfig) {
        NamedNodeMap predicateAttributes = childNode.getAttributes();
        String predicateType = getTextContent(predicateAttributes.getNamedItem("type"));
        String textContent = getTextContent(childNode);
        PredicateConfig predicateConfig = new PredicateConfig();
        if ("class-name".equals(predicateType)) {
            predicateConfig.setClassName(textContent);
        } else if ("sql".equals(predicateType)) {
            predicateConfig.setSql(textContent);
        }
        queryCacheConfig.setPredicateConfig(predicateConfig);
    }

    private void queryCacheIndexesHandle(Node n, QueryCacheConfig queryCacheConfig) {
        for (org.w3c.dom.Node indexNode : new AbstractXmlConfigHelper.IterableNodeList(n.getChildNodes())) {
            if ("index".equals(cleanNodeName(indexNode))) {
                final NamedNodeMap attrs = indexNode.getAttributes();
                boolean ordered = checkTrue(getTextContent(attrs.getNamedItem("ordered")));
                String attribute = getTextContent(indexNode);
                queryCacheConfig.addIndexConfig(new MapIndexConfig(attribute, ordered));
            }
        }
    }

}
