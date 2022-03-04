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
 * Contains classes for loading, parsing YAML documents and building a
 * YAML specific DOM of {@link com.hazelcast.internal.yaml.YamlNode} instances
 * <p>
 * The YAML documents are loaded and parsed with the external SnakeYaml
 * parser, which supports YAML 1.2 documents, and the JSON schema.
 */
package com.hazelcast.internal.yaml;
