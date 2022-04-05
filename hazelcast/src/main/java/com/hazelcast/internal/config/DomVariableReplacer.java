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

package com.hazelcast.internal.config;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.replacer.spi.ConfigReplacer;
import org.w3c.dom.Node;

/**
 * Interface for replacing variable in DOM {@link Node}s.
 */
interface DomVariableReplacer {

    /**
     * Replaces variables in the given {@link Node} with the provided
     * {@link ConfigReplacer}.
     *
     * @param node     The node in which the variables to be replaced
     * @param replacer The replacer to be used for replacing the variables
     * @param failFast Indicating whether or not a {@link
     *                 InvalidConfigurationException} should be thrown if no
     *                 replacement found for the variables in the node
     * @throws InvalidConfigurationException if no replacement is found for a
     *                                       variable and {@code failFast} is
     *                                       {@code }true
     */
    void replaceVariables(Node node, ConfigReplacer replacer, boolean failFast);
}
