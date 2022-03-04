/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.internal.config.ConfigUtils;
import com.hazelcast.internal.config.DomConfigHelper;
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
    protected final boolean strict;

    protected AbstractQueryCacheConfigBuilderHelper(boolean domLevel3, boolean strict) {
        this.domLevel3 = domLevel3;
        this.strict = strict;
    }

    protected AbstractQueryCacheConfigBuilderHelper(boolean domLevel3) {
        this.domLevel3 = domLevel3;
        this.strict = true;
    }

    protected String getTextContent(Node node) {
        return DomConfigHelper.getTextContent(node, domLevel3);
    }

    @SuppressWarnings({"checkstyle:cyclomaticcomplexity"})
    protected void populateQueryCacheConfig(QueryCacheConfig queryCacheConfig,
                                            Node childNode, String nodeName) {
        if (matches("entry-listeners", nodeName)) {
            handleEntryListeners(queryCacheConfig, childNode);
        } else if (matches("include-value", nodeName)) {
            boolean includeValue = getBooleanValue(getTextContent(childNode));
            queryCacheConfig.setIncludeValue(includeValue);
        } else if (matches("batch-size", nodeName)) {
            int batchSize = getIntegerValue("batch-size", getTextContent(childNode));
            queryCacheConfig.setBatchSize(batchSize);
        } else if (matches("buffer-size", nodeName)) {
            int bufferSize = getIntegerValue("buffer-size", getTextContent(childNode));
            queryCacheConfig.setBufferSize(bufferSize);
        } else if (matches("delay-seconds", nodeName)) {
            int delaySeconds = getIntegerValue("delay-seconds", getTextContent(childNode));
            queryCacheConfig.setDelaySeconds(delaySeconds);
        } else if (matches("in-memory-format", nodeName)) {
            queryCacheConfig.setInMemoryFormat(InMemoryFormat.valueOf(upperCaseInternal(getTextContent(childNode))));
        } else if (matches("coalesce", nodeName)) {
            boolean coalesce = getBooleanValue(getTextContent(childNode));
            queryCacheConfig.setCoalesce(coalesce);
        } else if (matches("populate", nodeName)) {
            boolean populate = getBooleanValue(getTextContent(childNode));
            queryCacheConfig.setPopulate(populate);
        } else if (matches("serialize-keys", nodeName)) {
            boolean serializeKeys = getBooleanValue(getTextContent(childNode));
            queryCacheConfig.setSerializeKeys(serializeKeys);
        } else if (matches("indexes", nodeName)) {
            queryCacheIndexesHandle(childNode, queryCacheConfig);
        } else if (matches("predicate", nodeName)) {
            queryCachePredicateHandler(childNode, queryCacheConfig);
        } else if (matches("eviction", nodeName)) {
            queryCacheConfig.setEvictionConfig(getEvictionConfig(childNode));
        }
    }

    private EvictionConfig getEvictionConfig(final Node node) {
        final EvictionConfig evictionConfig = new EvictionConfig();
        final Node size = getNamedItemNode(node, "size");
        final Node maxSizePolicy = getNamedItemNode(node, "max-size-policy");
        final Node evictionPolicy = getNamedItemNode(node, "eviction-policy");
        Node comparatorClassName = getNamedItemNode(node, "comparator-class-name");
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
            String nodeName = cleanNodeName(childNode);
            populateQueryCacheConfig(queryCacheConfig, childNode, nodeName);
        }
        clientConfig.addQueryCacheConfig(mapName, queryCacheConfig);
    }

    protected abstract String getCacheMapName(NamedNodeMap attrs);

    protected abstract void handleEntryListeners(QueryCacheConfig queryCacheConfig, Node childNode);

    protected void handleEntryListenerNode(QueryCacheConfig queryCacheConfig, Node listenerNode) {
        boolean incValue
                = getBooleanValue(getTextContent(getNamedItemNode(listenerNode, "include-value")));
        boolean local
                = getBooleanValue(getTextContent(getNamedItemNode(listenerNode, "local")));
        String listenerClass = getTextContent(listenerNode);
        queryCacheConfig.addEntryListenerConfig(
                new EntryListenerConfig(listenerClass, local, incValue));
    }

    protected abstract void queryCachePredicateHandler(Node childNode, QueryCacheConfig queryCacheConfig);

    protected abstract void queryCacheIndexesHandle(Node childNode, QueryCacheConfig queryCacheConfig);


    protected boolean matches(String config1, String config2) {
        return strict
                ? config1 != null && config1.equals(config2)
                : ConfigUtils.matches(config1, config2);
    }

    protected Node getNamedItemNode(final Node node, String attrName) {
        return getNamedItemNode(node.getAttributes(), attrName);
    }

    protected Node getNamedItemNode(final NamedNodeMap attrs, String attrName) {
        if (strict) {
            return attrs.getNamedItem(attrName);
        } else {
            Node attrNode = attrs.getNamedItem(attrName);
            return attrNode != null
                    ? attrNode
                    : attrs.getNamedItem(attrName.replace("-", ""));
        }
    }
}
