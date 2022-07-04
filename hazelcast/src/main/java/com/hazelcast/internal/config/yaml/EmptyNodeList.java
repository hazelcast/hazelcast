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

package com.hazelcast.internal.config.yaml;

import com.hazelcast.internal.yaml.YamlNode;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Empty {@link NodeList} implementation. Used by {@link YamlElementAdapter#getChildNodes()}
 * when wrapping {@link YamlNode}s with no children.
 */
final class EmptyNodeList implements NodeList {
    private static final NodeList INSTANCE = new EmptyNodeList();

    private EmptyNodeList() {
    }

    @Override
    public Node item(int index) {
        return null;
    }

    @Override
    public int getLength() {
        return 0;
    }

    /**
     * Returns the singleton instance of this class
     *
     * @return an empty {@link NodeList}
     * @see #INSTANCE
     */
    static NodeList emptyNodeList() {
        return INSTANCE;
    }
}
