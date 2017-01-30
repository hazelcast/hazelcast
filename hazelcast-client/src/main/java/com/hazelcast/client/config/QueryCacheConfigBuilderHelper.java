/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
        for (Node queryCacheNode : childElements(node)) {
            if ("query-cache".equals(cleanNodeName(queryCacheNode))) {
                NamedNodeMap attrs = queryCacheNode.getAttributes();
                String cacheName = getTextContent(attrs.getNamedItem("name"));
                String mapName = getTextContent(attrs.getNamedItem("mapName"));
                QueryCacheConfig queryCacheConfig = new QueryCacheConfig(cacheName);
                for (Node childNode : childElements(queryCacheNode)) {
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
            boolean includeValue = getBooleanValue(textContent);
            queryCacheConfig.setIncludeValue(includeValue);
        } else if ("batch-size".equals(nodeName)) {
            int batchSize = getIntegerValue("batch-size", textContent.trim()
            );
            queryCacheConfig.setBatchSize(batchSize);
        } else if ("buffer-size".equals(nodeName)) {
            int bufferSize = getIntegerValue("buffer-size", textContent.trim()
            );
            queryCacheConfig.setBufferSize(bufferSize);
        } else if ("delay-seconds".equals(nodeName)) {
            int delaySeconds = getIntegerValue("delay-seconds", textContent.trim()
            );
            queryCacheConfig.setDelaySeconds(delaySeconds);
        } else if ("in-memory-format".equals(nodeName)) {
            String value = textContent.trim();
            queryCacheConfig.setInMemoryFormat(InMemoryFormat.valueOf(upperCaseInternal(value)));
        } else if ("coalesce".equals(nodeName)) {
            boolean coalesce = getBooleanValue(textContent);
            queryCacheConfig.setCoalesce(coalesce);
        } else if ("populate".equals(nodeName)) {
            boolean populate = getBooleanValue(textContent);
            queryCacheConfig.setPopulate(populate);
        } else if ("indexes".equals(nodeName)) {
            queryCacheIndexesHandle(childNode, queryCacheConfig);
        } else if ("predicate".equals(nodeName)) {
            queryCachePredicateHandler(childNode, queryCacheConfig);
        } else if ("eviction".equals(nodeName)) {
            queryCacheConfig.setEvictionConfig(getEvictionConfig(childNode));
        }
    }

    private EvictionConfig getEvictionConfig(final Node node) {
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
        for (Node listenerNode : childElements(childNode)) {
            if ("entry-listener".equals(cleanNodeName(listenerNode))) {
                NamedNodeMap listenerNodeAttributes = listenerNode.getAttributes();
                boolean incValue
                        = getBooleanValue(getTextContent(listenerNodeAttributes.getNamedItem("include-value")));
                boolean local
                        = getBooleanValue(getTextContent(listenerNodeAttributes.getNamedItem("local")));
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
        for (Node indexNode : childElements(n)) {
            if ("index".equals(cleanNodeName(indexNode))) {
                final NamedNodeMap attrs = indexNode.getAttributes();
                boolean ordered = getBooleanValue(getTextContent(attrs.getNamedItem("ordered")));
                String attribute = getTextContent(indexNode);
                queryCacheConfig.addIndexConfig(new MapIndexConfig(attribute, ordered));
            }
        }
    }

}
