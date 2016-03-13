/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
 * This <code>ConfigPatternMatcher</code> supports a simplified wildcard matching.
 * See "Config.md ## Using Wildcard" for details about the syntax.
 * <p/>
 * In addition the candidates are weighted by the best match.
 * No exception will be thrown if multiple configurations are found.
 * The first best result is returned.
 * <p/>
 * This matcher represents a "contains" matching to provide backward compatibility.
 * <p/>
 * Please adapt your configuration and use {@link com.hazelcast.config.matcher.MatchingPointConfigPatternMatcher} instead.
 */
@Deprecated
public class LegacyMatchingPointConfigPatternMatcher implements ConfigPatternMatcher {

    @Override
    public String matches(Iterable<String> configPatterns, String itemName) throws ConfigurationException {
        String key = null;
        int lastMatchingPoint = -1;
        for (String pattern : configPatterns) {
            final int matchingPoint = getMatchingPoint(pattern, itemName);
            if (matchingPoint > lastMatchingPoint) {
                lastMatchingPoint = matchingPoint;
                key = pattern;
            }
        }
        return key;
    }

    /**
     * This method returns higher values the better the matching is.
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
        int indexFirstPart = itemName.indexOf(firstPart, 0);
        if (indexFirstPart == -1) {
            return -1;
        }

        String secondPart = pattern.substring(index + 1);
        int indexSecondPart = itemName.indexOf(secondPart, index + 1);
        if (indexSecondPart == -1) {
            return -1;
        }

        return firstPart.length() + secondPart.length();
    }
}
