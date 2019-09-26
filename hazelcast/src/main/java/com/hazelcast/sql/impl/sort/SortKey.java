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

package com.hazelcast.sql.impl.sort;

import java.util.List;

/**
 * Single sort key.
 */
public class SortKey {
    /** The key */
    private final List<Object> key;

    /** Index to make rows unique. */
    private final long idx;

    public SortKey(List<Object> key, long idx) {
        this.key = key;
        this.idx = idx;
    }

    public List<Object> getKey() {
        return key;
    }

    public long getIdx() {
        return idx;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(idx);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SortKey) {
            SortKey other = (SortKey) obj;

            return other.idx == ((SortKey) obj).idx;
        } else {
            return false;
        }
    }
}
