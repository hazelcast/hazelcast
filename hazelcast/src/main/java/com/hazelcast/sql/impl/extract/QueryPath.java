/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.extract;

import static com.hazelcast.query.QueryConstants.KEY_ATTRIBUTE_NAME;
import static com.hazelcast.query.QueryConstants.THIS_ATTRIBUTE_NAME;

public final class QueryPath {

    public static final String KEY = KEY_ATTRIBUTE_NAME.value();
    public static final String VALUE = THIS_ATTRIBUTE_NAME.value();

    private final boolean key;
    private final String path;

    private QueryPath(String path, boolean key) {
        this.path = path;
        this.key = key;
    }

    public String getPath() {
        return path;
    }

    public boolean isKey() {
        return key;
    }

    public boolean isTop() {
        return path != null;
    }

    public static QueryPath create(String originalPath) {
        String path;
        boolean key;

        if (KEY.equals(originalPath)) {
            path = null;
            key = true;
        } else if (VALUE.equals(originalPath)) {
            path = null;
            key = false;
        } else {
            String keyPath = extractKeyPath(originalPath);

            if (keyPath != null) {
                path = keyPath;
                key = true;
            } else {
                path = originalPath;
                key = false;
            }
        }

        return new QueryPath(path, key);
    }

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
}
