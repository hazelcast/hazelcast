/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl;

import static com.hazelcast.query.QueryConstants.KEY_ATTRIBUTE_NAME;

/**
 * Common SQL engine utility methods used by both "core" and "sql" modules.
 */
public class SqlUtils {
    /**
     * Extract child path from the complex key-based path. E.g. "__key.field" => "field".
     *
     * @param path Original path.
     * @return Path without the key attribute or {@code null} if not a key.
     */
    public static String extractKeyPath(String path) {
        String prefix = KEY_ATTRIBUTE_NAME.value() + ".";

        return path.startsWith(prefix) ? path.substring(prefix.length()) : null;
    }

    private SqlUtils() {
        // No-op.
    }
}
