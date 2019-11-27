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

import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapPartitionLostListenerConfig;
import com.hazelcast.config.MetadataPolicy;
import com.hazelcast.config.PartitioningStrategyConfig;
import com.hazelcast.function.BiConsumerEx;
import com.hazelcast.internal.config.DomConfigHelper;

import org.w3c.dom.Node;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.internal.config.DomConfigHelper.childElements;
import static com.hazelcast.internal.config.DomConfigHelper.cleanNodeName;
import static com.hazelcast.internal.config.DomConfigHelper.getBooleanValue;
import static com.hazelcast.internal.config.DomConfigHelper.getIntegerValue;
import static com.hazelcast.internal.config.DomConfigHelper.getTextContent;
import static com.hazelcast.internal.config.DomConfigHelper.streamOfChildElements;
import static com.hazelcast.internal.util.StringUtil.upperCaseInternal;

@SuppressWarnings("checkstyle:classdataabstractioncoupling")
public class MapConfigProcessor {
    private final Node node;
    private final boolean domLevel3;
    private final Map<String, BiConsumerEx<MapConfig, Node>> attributeActions = new HashMap<>();

    @SuppressWarnings({"checkstyle:methodlength", "checkstyle:executablestatementcount"})
    public MapConfigProcessor(Node node, boolean domLevel3) {
        this.node = node;
        this.domLevel3 = domLevel3;
        attributeActions.put(
            "backup-count",
            (config, child) ->
                config.setBackupCount(getIntegerValue("backup-count", getTextContent(child, domLevel3).trim()))
        );
        attributeActions.put(
            "metadata-policy",
            (config, child) ->
                config.setMetadataPolicy(MetadataPolicy.valueOf(upperCaseInternal(getTextContent(child, domLevel3).trim())))
        );
        attributeActions.put(
            "in-memory-format",
            (config, child) ->
                config.setInMemoryFormat(InMemoryFormat.valueOf(upperCaseInternal(getTextContent(child, domLevel3).trim())))
        );
        attributeActions.put(
            "async-backup-count",
            (config, child) ->
                config.setAsyncBackupCount(getIntegerValue("async-backup-count", getTextContent(child, domLevel3).trim()))
        );
        attributeActions.put(
            "eviction",
            (config, child) ->
                config.setEvictionConfig(new IMapEvictionConfigProcessor(child, domLevel3).process())
        );
        attributeActions.put(
            "time-to-live-seconds",
            (mapConfig, child) ->
                mapConfig.setTimeToLiveSeconds(getIntegerValue("time-to-live-seconds", getTextContent(child, domLevel3).trim()))
        );
        attributeActions.put(
            "max-idle-seconds",
            (mapConfig, child) ->
                mapConfig.setMaxIdleSeconds(getIntegerValue("max-idle-seconds", getTextContent(child, domLevel3).trim()))
        );
        attributeActions.put("map-store", (mapConfig, child) ->
            mapConfig.setMapStoreConfig(new MapStoreProcessor(child, domLevel3).process()));
        attributeActions.put(
            "near-cache",
            (mapConfig, child) ->
                mapConfig.setNearCacheConfig(new NearCacheConfigProcessor(child, domLevel3).process())
        );
        attributeActions.put(
            "merge-policy",
            (mapConfig, child) ->
                mapConfig.setMergePolicyConfig(new MergePolicyConfigProcessor(child, domLevel3).process()));
        attributeActions.put("merkle-tree", (mapConfig, child) ->
            mapConfig.setMerkleTreeConfig(new MerkleTreeConfigProcessor(child, domLevel3).process()));
        attributeActions.put(
            "event-journal",
            (mapConfig, child) ->
                mapConfig.setEventJournalConfig(new EventJournalConfigProcessor(child, domLevel3).process())
        );
        attributeActions.put(
            "hot-restart",
            (mapConfig, child) ->
                mapConfig.setHotRestartConfig(new HotRestartConfigProcessor(child, domLevel3).process())
        );
        attributeActions.put(
            "read-backup-data",
            (mapConfig, child) -> mapConfig.setReadBackupData(getBooleanValue(getTextContent(child, domLevel3).trim()))
        );
        attributeActions.put(
            "statistics-enabled",
            (mapConfig, child) -> mapConfig.setStatisticsEnabled(getBooleanValue(getTextContent(child, domLevel3).trim()))
        );
        attributeActions.put(
            "cache-deserialized-values",
            (mapConfig, child) ->
                mapConfig.setCacheDeserializedValues(CacheDeserializedValues.parseString(getTextContent(child, domLevel3).trim()))
        );
        attributeActions.put(
            "wan-replication-ref",
            (mapConfig, child) ->
                mapConfig.setWanReplicationRef(new WanReplicationRefProcessor(child, domLevel3).process())
        );
        attributeActions.put(
            "indexes",
            (mapConfig, child) ->
                streamOfChildElements(child)
                    .filter(indexNode -> "index".equals(cleanNodeName(indexNode)))
                    .map(indexNode -> new IndexProcessor(indexNode, domLevel3).process())
                    .forEach(mapConfig::addIndexConfig)
        );
        attributeActions.put(
            "attributes",
            (mapConfig, child) ->
                streamOfChildElements(child)
                    .filter(extractorNode -> "attribute".equals(cleanNodeName(extractorNode)))
                    .map(extractorNode -> new AttributeConfigProcessor(extractorNode, domLevel3).process())
                    .forEach(mapConfig::addAttributeConfig)
        );
        attributeActions.put(
            "entry-listeners",
            (mapConfig, child) ->
                streamOfChildElements(child)
                    .filter(listenerNode -> "entry-listener".equals(cleanNodeName(listenerNode)))
                    .map(listenerNode -> new EntryListenerProcessor(listenerNode, domLevel3).process())
                    .forEach(mapConfig::addEntryListenerConfig)
        );
        attributeActions.put(
            "partition-lost-listeners",
            (mapConfig, child) ->
                streamOfChildElements(child)
                    .filter(listenerNode -> "partition-lost-listener".equals(cleanNodeName(listenerNode)))
                    .map(listenerNode -> new MapPartitionLostListenerConfig(getTextContent(listenerNode, domLevel3)))
                    .forEach(mapConfig::addMapPartitionLostListenerConfig)
        );
        attributeActions.put(
            "partition-strategy",
            (mapConfig, child) ->
                mapConfig.setPartitioningStrategyConfig(new PartitioningStrategyConfig(getTextContent(child, domLevel3).trim()))
        );
        attributeActions.put(
            "split-brain-protection-ref",
            (mapConfig, child) -> mapConfig.setSplitBrainProtectionName(getTextContent(child, domLevel3).trim())
        );
        attributeActions.put(
            "query-caches",
            (mapConfig, child) ->
                streamOfChildElements(child)
                    .filter(queryCacheNode -> "query-cache".equals(cleanNodeName(queryCacheNode)))
                    .map(queryCacheNode -> new QueryCacheConfigProcessor(queryCacheNode, domLevel3).process())
                    .forEach(mapConfig::addQueryCacheConfig)
        );
    }

    public MapConfig process() {
        String name = DomConfigHelper.getAttribute(node, "name", domLevel3);
        MapConfig mapConfig = new MapConfig();
        mapConfig.setName(name);
        for (Node node : childElements(node)) {
            String nodeName = cleanNodeName(node);
            attributeActions.getOrDefault(
                nodeName,
                (config, nodeAndValue) -> {
                    throw new IllegalArgumentException("Node with name " + nodeName + " was not found");
                }
                ).accept(mapConfig, node);
        }
        return mapConfig;
    }
}
