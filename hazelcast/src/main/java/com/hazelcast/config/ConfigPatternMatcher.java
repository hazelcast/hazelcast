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

/**
 * The ConfigPatternMatcher provides a strategy to match an item name
 * to a configuration pattern.
 * <p>
 * It is used on each Config.getXXXConfig() and ClientConfig.getXXXConfig()
 * call for any data structure. It is supplied a list of config patterns
 * and the name of the item that the configuration is requested for.
 * <p>
 * Default matcher is {@link com.hazelcast.config.matcher.MatchingPointConfigPatternMatcher}.
 */
@FunctionalInterface
public interface ConfigPatternMatcher {

    /**
     * Returns the best match for an item name out of a list of configuration patterns.
     *
     * @param configPatterns    list of known configuration patterns
     * @param itemName          item name to match
     * @return                  a key of configPatterns which matches
     *                          the item name or {@code null} if there
     *                          is no match
     * @throws InvalidConfigurationException if ambiguous configurations are found
     */
    String matches(Iterable<String> configPatterns, String itemName)
            throws InvalidConfigurationException;
}
