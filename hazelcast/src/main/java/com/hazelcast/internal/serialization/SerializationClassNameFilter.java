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

package com.hazelcast.internal.serialization;

import com.hazelcast.config.ClassFilter;
import com.hazelcast.config.JavaSerializationFilterConfig;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.nio.serialization.ClassNameFilter;

import static java.lang.String.format;

/**
 * Implementation of basic protection against untrusted deserialization. It holds blacklist and whitelist with classnames and
 * package names.
 *
 * @see #filter(String)
 */
public final class SerializationClassNameFilter implements ClassNameFilter {

    private static final String DESERIALIZATION_ERROR = "Resolving class %s is not allowed.";
    private static final ClassFilter DEFAULT_WHITELIST;

    private final ClassFilter blacklist;
    private final ClassFilter whitelist;
    private final boolean useDefaultWhitelist;

    public SerializationClassNameFilter(JavaSerializationFilterConfig config) {
        Preconditions.checkNotNull(config, "JavaSerializationFilterConfig has to be provided");
        blacklist = config.getBlacklist();
        whitelist = config.getWhitelist();
        useDefaultWhitelist = !config.isDefaultsDisabled();
    }

    /**
     * Throws {@link SecurityException} if the given class name appears on the blacklist or does not appear a whitelist.
     *
     * @param className class name to check
     * @throws SecurityException if the classname is not allowed for deserialization
     */
    public void filter(String className) throws SecurityException {
        if (blacklist.isListed(className)) {
            throw new SecurityException(format(DESERIALIZATION_ERROR, className));
        }
        // if whitelisting is enabled (either explicit or as a default whitelist), force the whitelist check
        if (useDefaultWhitelist || !whitelist.isEmpty()) {
            if (whitelist.isListed(className)
                    || (useDefaultWhitelist && DEFAULT_WHITELIST.isListed(className))) {
                return;
            }
            throw new SecurityException(format(DESERIALIZATION_ERROR, className));
        }
    }

    static {
        DEFAULT_WHITELIST = new ClassFilter();
        DEFAULT_WHITELIST.addPrefixes("com.hazelcast.", "java", "[");
    }
}
