/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.dictionary.impl;

import com.hazelcast.dictionary.impl.type.EntryType;
import com.hazelcast.internal.memory.impl.UnsafeUtil;
import sun.misc.Unsafe;

public abstract class EntryEncoder<K, V> {

    protected final EntryType entryType;
    protected UnsafeSupport unsafeSupport = new UnsafeSupport();
    protected final Unsafe unsafe = UnsafeUtil.UNSAFE;

    public EntryEncoder(EntryType entryType) {
        this.entryType = entryType;
    }

    public abstract long writeKey(K key, long address);

    /**
     * Writes the entry to memory
     *
     * @param key
     * @param value
     * @param address the address to start writing to.
     * @param length  the length of the available memory to write to.
     * @return -1 if there was not enough space, otherwise the number of bytes written.
     */
    public abstract int writeEntry(K key, V value, long address, int length);

    public abstract long writeValue(V value, long entryAddress);

    public abstract V readValue(long address);

    public abstract K readKey(long address);

    public abstract boolean keyMatches(long address, K key);
}
