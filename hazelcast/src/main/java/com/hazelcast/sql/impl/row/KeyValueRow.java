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

package com.hazelcast.sql.impl.row;

import com.hazelcast.sql.impl.expression.KeyValueExtractorExpression;

import java.util.Arrays;
import java.util.List;

/**
 * Key-value row. Appears during iteration over a data stored in map or it's index.
 */
public class KeyValueRow implements Row {
    /** Null-marker. */
    private static final Object NULL = new Object();

    /** Extractor. */
    private final KeyValueRowExtractor extractor;

    /** Field expressions. */
    private final List<KeyValueExtractorExpression<?>> fieldExpressions;

    /** Cached objects. */
    private final Object[] cache;

    /** Key. */
    private Object key;

    /** Value. */
    private Object val;

    public KeyValueRow(KeyValueRowExtractor extractor, List<KeyValueExtractorExpression<?>> fieldExpressions) {
        this.extractor = extractor;
        this.fieldExpressions = fieldExpressions;

        cache = new Object[fieldExpressions.size()];
    }

    public void setKeyValue(Object key, Object val) {
        this.key = key;
        this.val = val;

        Arrays.fill(cache, null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getColumn(int idx) {
        Object res = cache[idx];

        if (res == null) {
            KeyValueExtractorExpression<?> fieldExpression = fieldExpressions.get(idx);

            res = fieldExpression.eval(this);

            cache[idx] = res != null ? res : NULL;
        } else if (res == NULL) {
            res = null;
        }

        return (T) res;
    }

    @Override
    public int getColumnCount() {
        return fieldExpressions.size();
    }

    /**
     * Extract the value with the given path.
     *
     * @param path Path.
     * @return Extracted value.
     */
    public Object extract(String path) {
        return extractor.extract(key, val, path);
    }
}
