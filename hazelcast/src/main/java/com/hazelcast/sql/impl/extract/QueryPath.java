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

import com.hazelcast.sql.impl.QueryException;

import static com.hazelcast.query.QueryConstants.KEY_ATTRIBUTE_NAME;
import static com.hazelcast.query.QueryConstants.THIS_ATTRIBUTE_NAME;

/**
 * Represent a path to the attribute within a key-value pair.
 */
public final class QueryPath {

    public static final String KEY = KEY_ATTRIBUTE_NAME.value();
    public static final String VALUE = THIS_ATTRIBUTE_NAME.value();

    private static final String KEY_PREFIX = KEY + ".";
    private static final String VALUE_PREFIX = VALUE + ".";

    private static final QueryPath KEY_PATH = new QueryPath(null, true);
    private static final QueryPath VALUE_PATH = new QueryPath(null, false);

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
        return path == null;
    }

    public static QueryPath create(String originalPath) {
        if (isEmpty(originalPath)) {
            throw badPathException(originalPath);
        }

        if (KEY.equals(originalPath)) {
            return KEY_PATH;
        } else if (VALUE.equals(originalPath)) {
            return VALUE_PATH;
        }

        if (originalPath.startsWith(KEY_PREFIX)) {
            String path = originalPath.substring(KEY_PREFIX.length());

            if (isEmpty(path)) {
                throw badPathException(originalPath);
            }

            return new QueryPath(path, true);
        } else if (originalPath.startsWith(VALUE_PREFIX)) {
            String path = originalPath.substring(VALUE_PREFIX.length());

            if (isEmpty(path)) {
                throw badPathException(originalPath);
            }

            return new QueryPath(path, false);
        } else {
            return new QueryPath(originalPath, false);
        }
    }

    private static boolean isEmpty(String path) {
        return path == null || path.isEmpty();
    }

    private static QueryException badPathException(String path) {
        throw QueryException.error("Field cannot be empty: " + path);
    }
}
