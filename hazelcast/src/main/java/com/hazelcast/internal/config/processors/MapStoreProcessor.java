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

import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.internal.config.DomConfigHelper;

import org.w3c.dom.Node;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static com.hazelcast.config.MapStoreConfig.DEFAULT_WRITE_COALESCING;
import static com.hazelcast.config.MapStoreConfig.InitialLoadMode;
import static com.hazelcast.internal.config.DomConfigHelper.cleanNodeName;
import static com.hazelcast.internal.config.DomConfigHelper.getBooleanValue;
import static com.hazelcast.internal.config.DomConfigHelper.getIntegerValue;
import static com.hazelcast.internal.config.DomConfigHelper.getTextContent;
import static com.hazelcast.internal.config.DomConfigHelper.streamOfAttributes;
import static com.hazelcast.internal.config.DomConfigHelper.streamOfChildElements;
import static com.hazelcast.internal.util.StringUtil.upperCaseInternal;

class MapStoreProcessor implements Processor<MapStoreConfig> {
    private final Node node;
    private final Map<String, BiConsumer<MapStoreConfig, Node>> map = new HashMap<>();

    MapStoreProcessor(Node node, boolean domLevel3) {
        this.node = node;
        map.put(
            "enabled",
            (mapStoreConfig, child) -> mapStoreConfig.setEnabled(getBooleanValue(getTextContent(child, domLevel3).trim())));
        map.put(
            "initial-mode",
            (mapStoreConfig, child) ->
                mapStoreConfig.setInitialLoadMode(InitialLoadMode.valueOf(upperCaseInternal(getTextContent(child, domLevel3)))));
        map.put(
            "class-name",
            (mapStoreConfig, child) -> mapStoreConfig.setClassName(getTextContent(child, domLevel3).trim())
        );
        map.put(
            "factory-class-name",
            (mapStoreConfig, child) -> mapStoreConfig.setFactoryClassName(getTextContent(child, domLevel3).trim())
        );
        map.put(
            "write-delay-seconds",
            (mapStoreConfig, child) ->
                mapStoreConfig.setWriteDelaySeconds(
                    getIntegerValue("write-delay-seconds", getTextContent(child, domLevel3).trim()))
        );
        map.put(
            "write-batch-size",
            (mapStoreConfig, child) ->
                mapStoreConfig.setWriteBatchSize(getIntegerValue("write-batch-size", getTextContent(child, domLevel3).trim()))
        );
        map.put(
            "write-coalescing",
            (mapStoreConfig, child) -> mapStoreConfig.setWriteCoalescing(
                    Optional.of(getTextContent(child, domLevel3))
                        .filter(s -> !s.isEmpty())
                        .map(DomConfigHelper::getBooleanValue)
                        .orElse(DEFAULT_WRITE_COALESCING))
        );
        map.put(
            "properties",
            (mapStoreConfig, child) -> Optional.ofNullable(mapStoreConfig.getProperties())
                    .ifPresent(properties -> streamOfChildElements(child)
                            .forEach(n -> properties.setProperty(key(n, domLevel3), getTextContent(n, domLevel3).trim())))
        );
    }

    private String key(Node node, boolean domLevel3) {
        final String name = cleanNodeName(node);
        return "property".equals(name)
            ? getTextContent(node.getAttributes().getNamedItem("name"), domLevel3).trim()
            // old way - probably should be deprecated
            : name;
    }

    @Override
    public MapStoreConfig process() {
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        Stream.concat(streamOfAttributes(node), streamOfChildElements(node))
            .forEach(node -> map.get(cleanNodeName(node)).accept(mapStoreConfig, node));
        return mapStoreConfig;
    }
}
