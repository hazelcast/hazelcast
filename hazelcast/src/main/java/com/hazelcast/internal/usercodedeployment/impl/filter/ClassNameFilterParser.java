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

package com.hazelcast.internal.usercodedeployment.impl.filter;

import com.hazelcast.config.UserCodeDeploymentConfig;
import com.hazelcast.internal.util.filter.AndFilter;
import com.hazelcast.internal.util.filter.Filter;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.internal.util.SetUtil.createHashSet;

public final class ClassNameFilterParser {

    private static final String[] BUILTIN_BLACKLIST_PREFIXES = {
            "javax.",
            "java.",
            "sun.",
            "com.hazelcast.",
    };

    private ClassNameFilterParser() {
    }

    public static Filter<String> parseClassNameFilters(UserCodeDeploymentConfig config) {
        Filter<String> classFilter = parseBlackList(config);
        String whitelistedPrefixes = config.getWhitelistedPrefixes();
        Set<String> whitelistSet = parsePrefixes(whitelistedPrefixes);
        if (!whitelistSet.isEmpty()) {
            ClassWhitelistFilter whitelistFilter = new ClassWhitelistFilter(whitelistSet.toArray(new String[0]));
            classFilter = new AndFilter<String>(classFilter, whitelistFilter);
        }
        return classFilter;
    }

    private static Filter<String> parseBlackList(UserCodeDeploymentConfig config) {
        String blacklistedPrefixes = config.getBlacklistedPrefixes();
        Set<String> blacklistSet = parsePrefixes(blacklistedPrefixes);
        blacklistSet.addAll(Arrays.asList(BUILTIN_BLACKLIST_PREFIXES));
        return new ClassBlacklistFilter(blacklistSet.toArray(new String[0]));
    }

    private static Set<String> parsePrefixes(String prefixes) {
        if (prefixes == null) {
            return new HashSet<String>();
        }

        prefixes = prefixes.trim();
        String[] prefixArray = prefixes.split(",");

        Set<String> blacklistSet = createHashSet(prefixArray.length + BUILTIN_BLACKLIST_PREFIXES.length);
        for (String prefix : prefixArray) {
            blacklistSet.add(prefix.trim());
        }
        return blacklistSet;
    }
}
