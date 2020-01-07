/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.config.impl;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.internal.config.DomConfigHelper;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.QueryCacheConfig;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import static com.hazelcast.internal.config.DomConfigHelper.childElements;
import static com.hazelcast.internal.config.DomConfigHelper.cleanNodeName;
import static com.hazelcast.internal.config.DomConfigHelper.getBooleanValue;
import static com.hazelcast.internal.config.DomConfigHelper.getIntegerValue;
import static com.hazelcast.internal.util.StringUtil.upperCaseInternal;

/**
 * Abstract class sharing logic between the two implementations of the
 * {@link QueryCacheConfigBuilderHelper} interface.
 *
 * @see QueryCacheXmlConfigBuilderHelper
 * @see QueryCacheYamlConfigBuilderHelper
 */
abstract class AbstractQueryCacheConfigBuilderHelper implements QueryCacheConfigBuilderHelper {
    protected final boolean domLevel3;

    protected AbstractQueryCacheConfigBuilderHelper(boolean domLevel3) {
        this.domLevel3 = domLevel3;
    }

    protected String getTextContent(Node node) {
        return DomConfigHelper.getTextContent(node, domLevel3);
    }

    protected void populateQueryCacheConfig(QueryCacheConfig queryCacheConfig,
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
        Node comparatorClassName = node.getAttributes().getNamedItem("comparator-class-name");
        if (size != null) {
            evictionConfig.setSize(Integer.parseInt(getTextContent(size)));
        }
        if (maxSizePolicy != null) {
            evictionConfig.setMaxSizePolicy(
                    MaxSizePolicy.valueOf(
                            upperCaseInternal(getTextContent(maxSizePolicy)))
            );
        }
        if (evictionPolicy != null) {
            evictionConfig.setEvictionPolicy(
                    EvictionPolicy.valueOf(
                            upperCaseInternal(getTextContent(evictionPolicy)))
            );
        }
        if (comparatorClassName != null) {
            evictionConfig.setComparatorClassName(getTextContent(comparatorClassName));
        }
        return evictionConfig;
    }

    protected abstract String getCacheName(Node queryCacheNode);

    protected void handleQueryCacheNode(ClientConfig clientConfig, Node queryCacheNode) {
        NamedNodeMap attrs = queryCacheNode.getAttributes();
        String cacheName = getCacheName(queryCacheNode);
        String mapName = getCacheMapName(attrs);
        QueryCacheConfig queryCacheConfig = new QueryCacheConfig(cacheName);
        for (Node childNode : childElements(queryCacheNode)) {
            String textContent = getTextContent(childNode);
            String nodeName = cleanNodeName(childNode);
            populateQueryCacheConfig(queryCacheConfig, childNode, textContent, nodeName);
        }
        clientConfig.addQueryCacheConfig(mapName, queryCacheConfig);
    }

    protected abstract String getCacheMapName(NamedNodeMap attrs);

    protected abstract void handleEntryListeners(QueryCacheConfig queryCacheConfig, Node childNode);

    protected void handleEntryListenerNode(QueryCacheConfig queryCacheConfig, Node listenerNode) {
        NamedNodeMap listenerNodeAttributes = listenerNode.getAttributes();
        boolean incValue
                = getBooleanValue(getTextContent(listenerNodeAttributes.getNamedItem("include-value")));
        boolean local
                = getBooleanValue(getTextContent(listenerNodeAttributes.getNamedItem("local")));
        String listenerClass = getTextContent(listenerNode);
        queryCacheConfig.addEntryListenerConfig(
                new EntryListenerConfig(listenerClass, local, incValue));
    }

    protected abstract void queryCachePredicateHandler(Node childNode, QueryCacheConfig queryCacheConfig);

    protected abstract void queryCacheIndexesHandle(Node childNode, QueryCacheConfig queryCacheConfig);
}
