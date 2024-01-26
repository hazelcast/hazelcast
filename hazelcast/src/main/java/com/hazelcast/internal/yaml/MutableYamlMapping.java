/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
 * Mutable interface of {@link YamlMapping}
 */
public interface MutableYamlMapping extends YamlMapping, MutableYamlNode {

    /**
     * Adds a new child node to the mapping with the provided name
     *
     * @param name The name of the new child
     * @param node The child node
     */
    void addChild(String name, YamlNode node);

    /**
     * Removes a child with the given name if exists
     *
     * @param name The name of the child to remove
     */
    void removeChild(String name);
}
