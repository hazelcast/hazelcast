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
 * Interface for YAML mapping nodes
 */
public interface YamlMapping extends YamlCollection {

    /**
     * Gets a child node by its name
     *
     * @param name the name of the child node
     * @return the child node with the given name if exists,
     * {@code null} otherwise
     */
    YamlNode child(String name);

    /**
     * Returns the children as {@link YamlNameNodePair}s
     * <p>
     * This method may have {@code null} values as the {@code childNode}
     * in the returned pairs if the node's values is explicitly defined
     * as {@code !!null} in the YAML document.
     * <p>
     * The difference from the {@link #children()} children method is
     * that {@link #children()} does not return {@code null} children
     * nodes, while {@link #childrenPairs()} does.
     *
     * @return an {@link Iterable} pair of node names and node instances
     */
    Iterable<YamlNameNodePair> childrenPairs();

    /**
     * Gets a child mapping node by its name
     *
     * @param name the name of the child node
     * @return the child mapping node with the given name if exists,
     * {@code null} otherwise
     */
    YamlMapping childAsMapping(String name);

    /**
     * Gets a child sequence node by its name
     *
     * @param name the name of the child node
     * @return the child sequence node with the given name if exists,
     * {@code null} otherwise
     */
    YamlSequence childAsSequence(String name);

    /**
     * Gets a child scalar node by its name
     *
     * @param name the name of the child node
     * @return the child scalar node with the given name if exists,
     * {@code null} otherwise
     */
    YamlScalar childAsScalar(String name);

    /**
     * Gets a child scalar node's value by its name
     * <p>
     * See {@link YamlScalar} for the possible types
     * <p>
     * Please note that if the scalar's type is not the expected type T,
     * a {@link ClassCastException} is thrown <strong>at the call site</strong>.
     *
     * @param name the name of the child node
     * @return the child scalar node's value with the given name if exists,
     * {@code null} otherwise
     * @see YamlScalar
     * @see #childAsScalarValue(String, Class)
     */
    <T> T childAsScalarValue(String name);

    /**
     * Gets a child scalar node's value by its name with type hinting
     * <p>
     * See {@link YamlScalar} for the possible types
     *
     * @param name the name of the child node
     * @param type the type that the scalar's value type to be validated
     *             against
     * @return the child scalar node's value with the given name
     * @throws YamlException if the scalar's value is not a type of T
     * @see YamlScalar
     */
    <T> T childAsScalarValue(String name, Class<T> type);
}
