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

package com.hazelcast.config.matcher;

import com.hazelcast.config.ConfigPatternMatcher;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.internal.config.ConfigUtils;

/**
 * This {@code ConfigPatternMatcher} supports a simplified wildcard matching.
 * See "Config.md ## Using Wildcard" for details about the syntax options.
 * <p>
 * In addition the candidates are weighted by the best match. The best result is returned.
 * Throws {@link com.hazelcast.config.InvalidConfigurationException} is multiple configurations are found.
 */
public class MatchingPointConfigPatternMatcher implements ConfigPatternMatcher {

    @Override
    public String matches(Iterable<String> configPatterns, String itemName) throws InvalidConfigurationException {
        String candidate = null;
        String duplicate = null;
        int lastMatchingPoint = -1;
        for (String pattern : configPatterns) {
            int matchingPoint = getMatchingPoint(pattern, itemName);
            if (matchingPoint > -1 && matchingPoint >= lastMatchingPoint) {
                if (matchingPoint == lastMatchingPoint) {
                    duplicate = candidate;
                } else {
                    duplicate = null;
                }
                lastMatchingPoint = matchingPoint;
                candidate = pattern;
            }
        }
        if (duplicate != null) {
            throw ConfigUtils.createAmbiguousConfigurationException(itemName, candidate, duplicate);
        }
        return candidate;
    }

    /**
     * This method returns the higher value the better the matching is.
     *
     * @param pattern  configuration pattern to match with
     * @param itemName item name to match
     * @return -1 if name does not match at all, zero or positive otherwise
     */
    private int getMatchingPoint(String pattern, String itemName) {
        int index = pattern.indexOf('*');
        if (index == -1) {
            return -1;
        }

        String firstPart = pattern.substring(0, index);
        if (!itemName.startsWith(firstPart)) {
            return -1;
        }

        String secondPart = pattern.substring(index + 1);
        if (!itemName.endsWith(secondPart)) {
            return -1;
        }

        return firstPart.length() + secondPart.length();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o != null && getClass() == o.getClass();
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

}
