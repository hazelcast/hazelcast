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

/**
 * Contains adapter and utility classes needed to adapt YAML DOM classes
 * as W3C DOM ones, making config builders that accept W3C DOM able to
 * build the config structure from YAML.
 * <p>
 * This package intentionally lacks fully implementing the  functionality
 * of the W3C DOM model. Only the parts that required by the Hazelcast
 * config processors are implemented.
 * <p>
 * The package contains the following adapters:
 * <ul>
 * <li>{@link com.hazelcast.internal.yaml.YamlNode} to {@link org.w3c.dom.Node}</li>
 * <li>{@link com.hazelcast.internal.yaml.YamlCollection} to {@link org.w3c.dom.NodeList}</li>
 * <li>{@link com.hazelcast.internal.yaml.YamlMapping} to {@link org.w3c.dom.NamedNodeMap}</li>
 * </ul>
 */
package com.hazelcast.internal.config.yaml;
