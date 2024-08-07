/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.nio.serialization.ClassNameFilter;

import static java.lang.String.format;

public class ReflectionClassNameFilter implements ClassNameFilter {
    private static final String LOAD_CLASS_ERROR = "Creation of class %s is not allowed.";
    private static final String[] DEFAULT_WHITELIST_PREFIX = new String[]{"com.hazelcast.", "java", "["};

    private final ClassFilter blacklist;
    private final ClassFilter whitelist;

    public ReflectionClassNameFilter(JavaSerializationFilterConfig config) {
        Preconditions.checkNotNull(config, "JavaReflectionFilterConfig has to be provided");
        blacklist = config.getBlacklist();
        whitelist = config.getWhitelist();
        if (!config.isDefaultsDisabled()) {
            whitelist.addPrefixes(DEFAULT_WHITELIST_PREFIX);
        }
    }

    @Override
    public void filter(String className) throws SecurityException {
        if (blacklist.isListed(className)) {
            throw new SecurityException(format(LOAD_CLASS_ERROR, className));
        }
        if (!whitelist.isListed(className)) {
            throw new SecurityException(format(LOAD_CLASS_ERROR, className));
        }
    }
}
