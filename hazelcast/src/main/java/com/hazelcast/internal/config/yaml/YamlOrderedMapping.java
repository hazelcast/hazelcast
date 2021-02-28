/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.yaml.YamlMapping;
import com.hazelcast.internal.yaml.YamlNode;

/**
 * An ordered view of a {@link YamlMapping}
 */
interface YamlOrderedMapping extends YamlMapping {

    /**
     * Gets a child node by its index
     *
     * @param index the index of the child node
     * @return the child node with the given index if exists,
     * {@code null} otherwise
     */
    YamlNode child(int index);
}
