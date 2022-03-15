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

package com.hazelcast.config.replacer.spi;

import java.util.Properties;

/**
 * Interface to be implemented by pluggable variable replacers for the configuration files. The replacers can be configured in
 * XML configuration files and they are used to replace custom strings during loading the configuration.
 * <p>
 * A Variable to be replaced within the configuration file has following form:
 *
 * <pre>
 * "$" PREFIX "{" MASKED_VALUE "}"
 * </pre>
 *
 * where the {@code PREFIX} is the value returned by {@link #getPrefix()} method and {@code MASKED_VALUE} is a value provided to
 * the {@link #getReplacement(String)} method. The result of {@link #getReplacement(String)} method call replaces the whole
 * Variable String.
 */
public interface ConfigReplacer {

    /**
     * Initialization method called before first usage of the config replacer.
     *
     * @param properties properties configured (not {@code null})
     */
    void init(Properties properties);

    /**
     * Variable replacer prefix string. The value returned should be a constant unique short alphanumeric string without
     * whitespaces.
     *
     * @return constant prefix of this replacer
     */
    String getPrefix();

    /**
     * Provides String which should be used as a variable replacement for given masked value.
     *
     * @param maskedValue the masked value
     * @return either not {@code null} String to be used as a replacement for the variable; or null when this replacer is not
     *         able to handle the masked value.
     */
    String getReplacement(String maskedValue);
}
