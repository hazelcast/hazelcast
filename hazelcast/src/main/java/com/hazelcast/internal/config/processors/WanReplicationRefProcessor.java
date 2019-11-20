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

import com.hazelcast.config.WanReplicationRef;

import org.w3c.dom.Node;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.config.DomConfigHelper.cleanNodeName;
import static com.hazelcast.internal.config.DomConfigHelper.getAttribute;
import static com.hazelcast.internal.config.DomConfigHelper.getBooleanValue;
import static com.hazelcast.internal.config.DomConfigHelper.getTextContent;
import static com.hazelcast.internal.config.DomConfigHelper.streamOfChildElements;

class WanReplicationRefProcessor implements Processor<WanReplicationRef> {
    private final Node node;
    private final boolean domLevel3;
    private final Map<String, BiConsumer<WanReplicationRef, Node>> map = new HashMap<>();

    WanReplicationRefProcessor(Node node, boolean domLevel3) {
        this.node = node;
        this.domLevel3 = domLevel3;
        map.put(
            "merge-policy",
            (wanReplicationRef, child) -> wanReplicationRef.setMergePolicy(getTextContent(child, domLevel3))
        );
        map.put(
            "republishing-enabled",
            (wanReplicationRef, child) ->
                wanReplicationRef.setRepublishingEnabled(getBooleanValue(getTextContent(child, domLevel3)))
        );
        map.put(
            "filters",
            (wanReplicationRef, child) -> streamOfChildElements(child)
                    .filter(filter -> "filter-impl".equals(cleanNodeName(filter)))
                    .forEach(filter -> wanReplicationRef.addFilter(getTextContent(filter, domLevel3)))
        );
    }

    @Override
    public WanReplicationRef process() {
        WanReplicationRef wanReplicationRef = new WanReplicationRef();
        wanReplicationRef.setName(name());
        streamOfChildElements(node)
            .forEach(child -> map.get(cleanNodeName(child)).accept(wanReplicationRef, child));
        return wanReplicationRef;
    }

    private String name() {
        return getAttribute(node, "name", domLevel3);
    }
}
