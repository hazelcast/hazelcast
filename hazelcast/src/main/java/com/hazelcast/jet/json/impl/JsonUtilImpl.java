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

package com.hazelcast.jet.json.impl;

import com.hazelcast.config.ClassFilter;

import javax.sql.DataSource;

public final class JsonUtilImpl {
    /**
     * If the default classes should not be added to blocklist for JSON deserialization.
     * Disabling defaults is insecure.
     */
    static final String JSON_BLOCKLIST_DEFAULTS_DISABLED_PROPERTY = "hazelcast.jet.json.blocklist.defaultsDisabled";
    static final ClassFilter JSON_DEFAULT_BLOCKLIST = createJsonDefaultBlocklist();
    static final String JSON_DEFAULT_BLOCKLIST_HINT = String.format(
            "%nThis restriction can be disabled by setting property `%s` to true", JSON_BLOCKLIST_DEFAULTS_DISABLED_PROPERTY);

    private JsonUtilImpl() {
    }

    private static ClassFilter createJsonDefaultBlocklist() {
        ClassFilter blockList = new ClassFilter();
        blockList.addPrefixes(
                "com.hazelcast.shaded",
                "com.hazelcast.internal",
                "com.hazelcast.map.impl",
                "org.springframework.context",
                "org.springframework.beans"
        );
        blockList.addPackages(
                "com.hazelcast.config",
                "com.hazelcast.client.config"
        );
        return blockList;
    }

    static boolean isBlockedByDefaultBlocklist(Class<?> type) {
        return DataSource.class.isAssignableFrom(type)
                || JSON_DEFAULT_BLOCKLIST.isListed(type.getName());
    }
}
