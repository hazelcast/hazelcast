/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization;

import com.hazelcast.config.ClassFilter;
import com.hazelcast.config.JavaSerializationFilterConfig;
import com.hazelcast.nio.serialization.ClassNameFilter;

import javax.annotation.Nullable;

import static java.lang.String.format;

public class ReflectionClassNameFilter implements ClassNameFilter {
    private static final String LOAD_CLASS_ERROR = "Creation of class %s is not allowed.";
    private static final String[] DEFAULT_WHITELIST_PREFIX = new String[]{"com.hazelcast.", "java", "["};
    private static final String[] DEFAULT_BLOCKLIST_PREFIX = new String[]{"com.hazelcast.shaded."};

    private final ClassFilter blacklist;
    private final ClassFilter whitelist;

    public ReflectionClassNameFilter(@Nullable JavaSerializationFilterConfig config) {
        if (config != null) {
            blacklist = config.getBlacklist();
            whitelist = config.getWhitelist();
            if (!config.isDefaultsDisabled()) {
                whitelist.addPrefixes(DEFAULT_WHITELIST_PREFIX);
                addDefaultBlocklist();
            }
        } else {
            // if the filter is not explicitly configured, use only defaults for blocked classes
            // all classes that are not blocked, are allowed
            whitelist = null;
            blacklist = new ClassFilter();
            addDefaultBlocklist();
        }
    }

    private void addDefaultBlocklist() {
        blacklist.addPrefixes(DEFAULT_BLOCKLIST_PREFIX);
        blacklist.addClasses(
                // See: https://hazelcast.atlassian.net/browse/CTT-1112 and https://hazelcast.atlassian.net/browse/CTT-1090
                "com.zaxxer.hikari.HikariDataSource"
        );
    }

    @Override
    public void filter(String className) throws SecurityException {
        if (blacklist.isListed(className)) {
            throw new SecurityException(format(LOAD_CLASS_ERROR, className));
        }
        if (whitelist != null && !whitelist.isListed(className)) {
            throw new SecurityException(format(LOAD_CLASS_ERROR, className));
        }
    }
}
