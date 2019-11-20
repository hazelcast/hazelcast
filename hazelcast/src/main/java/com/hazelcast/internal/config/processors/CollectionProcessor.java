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

import org.w3c.dom.Node;

import java.util.function.Consumer;
import java.util.function.Function;

import static com.hazelcast.internal.config.DomConfigHelper.cleanNodeName;
import static com.hazelcast.internal.config.DomConfigHelper.streamOfChildElements;

class CollectionProcessor<T> {
    private final Node node;
    private final String tag;
    private final Function<Node, Processor<T>> itemProcessor;
    private final Consumer<T> configConsumer;

    CollectionProcessor(Node node, String tag, Function<Node, Processor<T>> itemProcessor, Consumer<T> configConsumer) {
        this.node = node;
        this.tag = tag;
        this.itemProcessor = itemProcessor;
        this.configConsumer = configConsumer;
    }

    public void process() {
        streamOfChildElements(node)
            .filter(indexNode -> tag.equals(cleanNodeName(indexNode)))
            .map(itemProcessor)
            .map(Processor::process)
            .forEach(configConsumer);
    }
}
