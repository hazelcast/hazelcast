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
 * Interface for YAML scalar nodes
 * <p>
 * The following types are supported:
 * <ul>
 * <li>String</li>
 * <li>Integer</li>
 * <li>Float</li>
 * <li>Boolean</li>
 * </ul>
 */
public interface YamlScalar extends YamlNode {
    /**
     * Checks if the value of this node is the given type
     *
     * @param type the {@link Class} instance of the type to check
     * @param <T>  the type to check
     * @return true if the value of the node is instance of the given type
     */
    <T> boolean isA(Class<T> type);

    /**
     * Gets the value of the node
     * <p>
     * Please note that if the scalar's type is not the expected type T,
     * a {@link ClassCastException} is thrown <strong>at the call site</strong>.
     *
     * @param <T> the expected type of the node
     * @return the value of the node
     */
    <T> T nodeValue();

    /**
     * Gets the value of the node with validating its type against the
     * provided type
     *
     * @param <T> the expected type of the node
     * @return the value of the node
     * @throws YamlException if the scalar's value is not a type of T
     */
    <T> T nodeValue(Class<T> type);
}
