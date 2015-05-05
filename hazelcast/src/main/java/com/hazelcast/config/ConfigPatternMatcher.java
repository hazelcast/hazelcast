/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
 * The ConfigPatternMatcher provides a strategy to match an item name to a configuration pattern.
 * <p/>
 * It is used on each Config.getXXXConfig() and ClientConfig.getXXXConfig() call for map, list, queue, set, executor, topic,
 * semaphore etc., so for example <code>itemName</code> is the name of a map and <code>configPatterns</code> are all defined
 * map configurations.
 * <p/>
 * If no configuration is found by the matcher it should return <tt>null</tt>. In this case the default config will be used
 * for this item then. If multiple configurations are found by the matcher a {@link com.hazelcast.config.ConfigurationException}
 * should be thrown.
 * <p/>
 * Since Hazelcast 3.5 the default matcher is {@link com.hazelcast.config.matcher.MatchingPointConfigPatternMatcher}.
 */
public interface ConfigPatternMatcher {

    /**
     * Returns the best match for an item name out of a list of configuration patterns.
     *
     * @param configPatterns    list of configuration patterns
     * @param itemName          item name to match
     * @return a key of configPatterns which matches the item name or <tt>null</tt> if nothing matches
     * @throws ConfigurationException if ambiguous configurations are found
     */
    String matches(Iterable<String> configPatterns, String itemName) throws ConfigurationException;
}
