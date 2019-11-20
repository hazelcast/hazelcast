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
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import org.w3c.dom.Node;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy;
import static com.hazelcast.internal.config.DomConfigHelper.cleanNodeName;
import static com.hazelcast.internal.config.DomConfigHelper.getAttribute;
import static com.hazelcast.internal.config.DomConfigHelper.getBooleanValue;
import static com.hazelcast.internal.config.DomConfigHelper.getIntegerValue;
import static com.hazelcast.internal.config.DomConfigHelper.getTextContent;
import static com.hazelcast.internal.config.DomConfigHelper.streamOfChildElements;
import static com.hazelcast.internal.util.StringUtil.upperCaseInternal;

class NearCacheConfigProcessor implements Processor<NearCacheConfig> {
    private static final ILogger LOGGER = Logger.getLogger(NearCacheConfigProcessor.class);
    private final Node node;
    private final boolean domLevel3;
    private final Map<String, BiConsumer<NearCacheConfig, Node>> map = new HashMap<>();

    NearCacheConfigProcessor(Node node, boolean domLevel3) {
        this.node = node;
        this.domLevel3 = domLevel3;
        map.put(
            "time-to-live-seconds",
            (nearCacheConfig, child) ->
                nearCacheConfig.setTimeToLiveSeconds(
                    getIntegerValue("time-to-live-seconds", getTextContent(child, domLevel3).trim()))
        );
        map.put(
            "max-idle-seconds",
            (nearCacheConfig, child) ->
                nearCacheConfig.setMaxIdleSeconds(getIntegerValue("max-idle-seconds", getTextContent(child, domLevel3).trim()))
        );
        map.put(
            "in-memory-format",
            (nearCacheConfig, child) ->
                nearCacheConfig.setInMemoryFormat(
                    InMemoryFormat.valueOf(upperCaseInternal(getTextContent(child, domLevel3).trim())))
        );
        map.put(
            "serialize-keys",
            (nearCacheConfig, child) ->
                nearCacheConfig.setSerializeKeys(getBooleanValue(getTextContent(child, domLevel3).trim()))
        );
        map.put(
            "invalidate-on-change",
            (nearCacheConfig, child) ->
                nearCacheConfig.setInvalidateOnChange(getBooleanValue(getTextContent(child, domLevel3).trim()))
        );
        map.put(
            "cache-local-entries",
            (nearCacheConfig, child) ->
                nearCacheConfig.setCacheLocalEntries(getBooleanValue(getTextContent(child, domLevel3).trim()))
        );
        map.put(
            "local-update-policy",
            (nearCacheConfig, child) ->
                nearCacheConfig.setLocalUpdatePolicy(LocalUpdatePolicy.valueOf(getTextContent(child, domLevel3).trim()))
        );
        map.put(
            "eviction",
            (nearCacheConfig, child) ->
                nearCacheConfig.setEvictionConfig(new NearCacheEvictionConfigProcessor(child, domLevel3).process())
        );
    }

    @Override
    public NearCacheConfig process() {
        NearCacheConfig nearCacheConfig = new NearCacheConfig(name());
        streamOfChildElements(node)
            .forEach(child -> map.get(cleanNodeName(child)).accept(nearCacheConfig, child));
        if (nearCacheConfig.isSerializeKeys() && nearCacheConfig.getInMemoryFormat() == InMemoryFormat.NATIVE) {
            LOGGER.warning(
                "The Near Cache doesn't support keys by-reference with NATIVE in-memory-format."
                    + " This setting will have no effect!");
        }
        return nearCacheConfig;
    }

    private String name() {
        return getAttribute(node, "name", domLevel3);
    }
}
