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

package com.hazelcast.config;

import com.hazelcast.internal.config.AbstractConfigRecognizer;
import com.hazelcast.internal.config.MemberXmlConfigRootTagRecognizer;
import com.hazelcast.internal.config.MemberYamlConfigRootTagRecognizer;

import java.util.LinkedList;
import java.util.List;

import static java.util.Arrays.asList;

/**
 * {@link ConfigRecognizer} implementation that recognizes Hazelcast
 * member declarative configurations based on an extensible set of
 * built-in {@link ConfigRecognizer} implementations.
 */
public class MemberConfigRecognizer extends AbstractConfigRecognizer {

    /**
     * Constructs an instance with the built-in set of
     * {@link ConfigRecognizer} implementations only.
     *
     * @throws Exception If there is an unexpected error occur during
     *                   instantiation.
     */
    public MemberConfigRecognizer() throws Exception {
        super(builtInRecognizers());
    }

    /**
     * Constructs an instance with the built-in set of
     * {@link ConfigRecognizer} implementations extended with ones
     * provided in {@code customRecognizers}.
     *
     * @param customRecognizers The custom config recognizers to use
     *                          besides the built-in ones.
     * @throws Exception If there is an unexpected error occur during
     *                   instantiation.
     */
    public MemberConfigRecognizer(ConfigRecognizer... customRecognizers) throws Exception {
        super(recognizers(customRecognizers));
    }

    private static List<ConfigRecognizer> recognizers(ConfigRecognizer... customRecognizers) throws Exception {
        List<ConfigRecognizer> configRecognizers = new LinkedList<>(builtInRecognizers());
        configRecognizers.addAll(asList(customRecognizers));
        return configRecognizers;
    }

    private static List<ConfigRecognizer> builtInRecognizers() throws Exception {
        return asList(
                new MemberXmlConfigRootTagRecognizer(),
                new MemberYamlConfigRootTagRecognizer());
    }
}
