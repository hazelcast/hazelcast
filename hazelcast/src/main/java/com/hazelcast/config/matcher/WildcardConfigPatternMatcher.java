/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config.matcher;

import com.hazelcast.config.ConfigPatternMatcher;
import com.hazelcast.config.ConfigurationException;

/**
 * This {@code ConfigPatternMatcher} supports a simplified wildcard matching.
 * See "Config.md ## Using Wildcard" for details about the syntax options.
 * <p>
 * Throws {@link com.hazelcast.config.ConfigurationException} is multiple configurations are found.
 */
public class WildcardConfigPatternMatcher implements ConfigPatternMatcher {

    @Override
    public String matches(Iterable<String> configPatterns, String itemName) throws ConfigurationException {
        String candidate = null;
        for (String pattern : configPatterns) {
            if (matches(pattern, itemName)) {
                if (candidate != null) {
                    throw new ConfigurationException(itemName, candidate, pattern);
                }
                candidate = pattern;
            }
        }
        return candidate;
    }

    /**
     * This method is public to be accessible by {@link com.hazelcast.security.permission.InstancePermission}.
     *
     * @param pattern  configuration pattern to match with
     * @param itemName item name to match
     * @return {@code true} if itemName matches, {@code false} otherwise
     */
    public boolean matches(String pattern, String itemName) {
        final int index = pattern.indexOf('*');
        if (index == -1) {
            return itemName.equals(pattern);
        }

        final String firstPart = pattern.substring(0, index);
        if (!itemName.startsWith(firstPart)) {
            return false;
        }

        final String secondPart = pattern.substring(index + 1);
        if (!itemName.endsWith(secondPart)) {
            return false;
        }

        return true;
    }
}
