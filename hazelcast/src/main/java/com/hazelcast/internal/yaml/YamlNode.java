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

package com.hazelcast.internal.yaml;

/**
 * Generic YAML node interface
 */
public interface YamlNode {
    String UNNAMED_NODE = "<unnamed>";

    /**
     * Returns the parent of the given node
     *
     * @return the parent node if exists, <code>null</code> otherwise
     */
    YamlNode parent();

    /**
     * Returns the name of the node
     *
     * @return the name of the node or {@link #UNNAMED_NODE} if not available
     */
    String nodeName();

    /**
     * Returns the path of the node from the root of the YAML structure.
     *
     * @return the path of the node
     */
    String path();

}
