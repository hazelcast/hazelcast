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
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.PredicateConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.query.impl.IndexUtils;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import static com.hazelcast.internal.config.DomConfigHelper.childElements;
import static com.hazelcast.internal.config.DomConfigHelper.getBooleanValue;

/**
 * YAML-specific implementation of the {@link QueryCacheConfigBuilderHelper}
 * interface. Builds {@link QueryCacheConfig} from YAML configuration.
 */
final class QueryCacheYamlConfigBuilderHelper extends AbstractQueryCacheConfigBuilderHelper {
    QueryCacheYamlConfigBuilderHelper() {
        super(true);
    }

    QueryCacheYamlConfigBuilderHelper(boolean strict) {
        super(true, strict);
    }

    @Override
    public void handleQueryCache(ClientConfig clientConfig, Node node) {
        for (Node queryCacheNode : childElements(node)) {
            handleQueryCacheNode(clientConfig, queryCacheNode);
        }
    }

    @Override
    protected String getCacheName(Node queryCacheNode) {
        return queryCacheNode.getNodeName();
    }

    @Override
    protected String getCacheMapName(NamedNodeMap attrs) {
        return getTextContent(getNamedItemNode(attrs, "map-name"));
    }

    @Override
    protected void handleEntryListeners(QueryCacheConfig queryCacheConfig, Node childNode) {
        for (Node listenerNode : childElements(childNode)) {
            boolean incValue = getBooleanValue(getTextContent(getNamedItemNode(listenerNode, "include-value")));
            boolean local = getBooleanValue(getTextContent(getNamedItemNode(listenerNode, "local")));
            String listenerClass = getTextContent(getNamedItemNode(listenerNode, "class-name"));
            queryCacheConfig.addEntryListenerConfig(new EntryListenerConfig(listenerClass, local, incValue));
        }
    }

    @Override
    protected void queryCachePredicateHandler(Node childNode, QueryCacheConfig queryCacheConfig) {
        Node classNameNode = getNamedItemNode(childNode, "class-name");
        Node sqlNode = getNamedItemNode(childNode, "sql");

        if (classNameNode != null && sqlNode != null) {
            throw new InvalidConfigurationException("Both class-name and sql is defined for the predicate of map "
                    + childNode.getParentNode().getParentNode().getNodeName());
        }

        if (classNameNode == null && sqlNode == null) {
            throw new InvalidConfigurationException("Either class-name and sql must be defined for the predicate of map "
                    + childNode.getParentNode().getParentNode().getNodeName());
        }

        PredicateConfig predicateConfig = new PredicateConfig();
        if (classNameNode != null) {
            predicateConfig.setClassName(getTextContent(classNameNode));
        } else if (sqlNode != null) {
            predicateConfig.setSql(getTextContent(sqlNode));
        }
        queryCacheConfig.setPredicateConfig(predicateConfig);
    }

    @Override
    protected void queryCacheIndexesHandle(Node childNode, QueryCacheConfig queryCacheConfig) {
        for (Node indexNode : childElements(childNode)) {
            IndexConfig indexConfig = IndexUtils.getIndexConfigFromYaml(indexNode, domLevel3, strict);

            queryCacheConfig.addIndexConfig(indexConfig);
        }
    }
}
