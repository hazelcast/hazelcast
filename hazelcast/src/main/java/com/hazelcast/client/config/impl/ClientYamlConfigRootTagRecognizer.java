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

package com.hazelcast.client.config.impl;

import com.hazelcast.config.ConfigRecognizer;
import com.hazelcast.internal.config.AbstractYamlConfigRootTagRecognizer;

/**
 * This {@link ConfigRecognizer} implementation recognizes Hazelcast
 * client YAML configuration by checking if the defined root tag is
 * "hazelcast-client" or not. For the implementation details please refer
 * to the {@link AbstractYamlConfigRootTagRecognizer} documentation.
 */
public class ClientYamlConfigRootTagRecognizer extends AbstractYamlConfigRootTagRecognizer {
    public ClientYamlConfigRootTagRecognizer() {
        super(ClientConfigSections.HAZELCAST_CLIENT.getName());
    }
}
