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
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.PredicateConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.query.impl.IndexUtils;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import static com.hazelcast.internal.config.DomConfigHelper.childElements;
import static com.hazelcast.internal.config.DomConfigHelper.cleanNodeName;

/**
 * XML-specific implementation of the {@link QueryCacheConfigBuilderHelper}
 * interface. Builds {@link QueryCacheConfig} from XML configuration.
 */
final class QueryCacheXmlConfigBuilderHelper extends AbstractQueryCacheConfigBuilderHelper {

    QueryCacheXmlConfigBuilderHelper(boolean domLevel3) {
        super(domLevel3);
    }

    @Override
    public void handleQueryCache(ClientConfig clientConfig, Node node) {
        for (Node queryCacheNode : childElements(node)) {
            if ("query-cache".equals(cleanNodeName(queryCacheNode))) {
                handleQueryCacheNode(clientConfig, queryCacheNode);
            }
        }
    }

    @Override
    protected String getCacheName(Node queryCacheNode) {
        NamedNodeMap attrs = queryCacheNode.getAttributes();
        return getTextContent(attrs.getNamedItem("name"));
    }

    @Override
    protected String getCacheMapName(NamedNodeMap attrs) {
        return getTextContent(attrs.getNamedItem("mapName"));
    }

    protected void handleEntryListeners(QueryCacheConfig queryCacheConfig, Node childNode) {
        for (Node listenerNode : childElements(childNode)) {
            if ("entry-listener".equals(cleanNodeName(listenerNode))) {
                handleEntryListenerNode(queryCacheConfig, listenerNode);
            }
        }
    }

    protected void queryCachePredicateHandler(Node childNode, QueryCacheConfig queryCacheConfig) {
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

    protected void queryCacheIndexesHandle(Node n, QueryCacheConfig queryCacheConfig) {
        for (Node indexNode : childElements(n)) {
            if ("index".equals(cleanNodeName(indexNode))) {
                IndexConfig indexConfig = IndexUtils.getIndexConfigFromXml(indexNode, domLevel3);

                queryCacheConfig.addIndexConfig(indexConfig);
            }
        }
    }

}
