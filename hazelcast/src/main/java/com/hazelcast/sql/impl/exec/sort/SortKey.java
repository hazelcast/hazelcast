/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.exec.sort;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;

/**
 * Single sort key.
 */
@SuppressFBWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class SortKey {
    /** The key */
    private final Object[] key;

    /** Index to make rows unique. */
    private final long index;

    public SortKey(Object[] key, long index) {
        this.key = key;
        this.index = index;
    }

    public Object[] getKey() {
        return key;
    }

    public long getIndex() {
        return index;
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(index);
        result = 31 * result + Arrays.hashCode(key);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SortKey) {
            SortKey other = (SortKey) obj;

            return index == other.index && Arrays.equals(key, other.key);
        } else {
            return false;
        }
    }
}
