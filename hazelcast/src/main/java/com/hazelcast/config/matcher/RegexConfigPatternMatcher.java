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

import java.util.regex.Pattern;

/**
 * This {@code ConfigPatternMatcher} uses Java regular expressions for matching.
 * <p>
 * Throws {@link com.hazelcast.config.InvalidConfigurationException} is multiple configurations are found.
 */
public class RegexConfigPatternMatcher implements ConfigPatternMatcher {

    private final int flags;

    public RegexConfigPatternMatcher() {
        this(0);
    }

    public RegexConfigPatternMatcher(int flags) {
        this.flags = flags;
    }

    @Override
    public String matches(Iterable<String> configPatterns, String itemName) throws InvalidConfigurationException {
        String candidate = null;
        for (String pattern : configPatterns) {
            if (Pattern.compile(pattern, flags).matcher(itemName).find()) {
                if (candidate != null) {
                    throw ConfigUtils.createAmbiguousConfigurationException(itemName, candidate, pattern);
                }
                candidate = pattern;
            }
        }
        return candidate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RegexConfigPatternMatcher that = (RegexConfigPatternMatcher) o;

        return flags == that.flags;
    }

    @Override
    public int hashCode() {
        return flags;
    }
}
