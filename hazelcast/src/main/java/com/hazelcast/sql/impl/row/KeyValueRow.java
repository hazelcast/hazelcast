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

package com.hazelcast.sql.impl.row;

/**
 * Key-value row. Appears during iteration over a data stored in map or it's index.
 */
public class KeyValueRow implements Row {
    /** Extractor. */
    private final KeyValueRowExtractor extractor;

    /** Key. */
    private Object key;

    /** Value. */
    private Object val;

    public KeyValueRow(KeyValueRowExtractor extractor) {
        this.extractor = extractor;
    }

    public void setKeyValue(Object key, Object val) {
        this.key = key;
        this.val = val;
    }

    @Override
    public <T> T getColumn(int idx) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getColumnCount() {
        throw new UnsupportedOperationException();
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
