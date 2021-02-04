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

package com.hazelcast.internal.config;

import com.hazelcast.config.ConfigRecognizer;

/**
 * This {@link ConfigRecognizer} implementation recognizes Hazelcast
 * member XML configuration by checking if the defined root tag is
 * "hazelcast" or not. For the implementation details please refer to
 * the {@link AbstractXmlConfigRootTagRecognizer} documentation.
 */
public class MemberXmlConfigRootTagRecognizer extends AbstractXmlConfigRootTagRecognizer {
    public MemberXmlConfigRootTagRecognizer() throws Exception {
        super(ConfigSections.HAZELCAST.getName());
    }
}
