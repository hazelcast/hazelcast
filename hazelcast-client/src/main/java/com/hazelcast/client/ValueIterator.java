/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.client;

import java.util.Iterator;
import java.util.Map.Entry;

public class ValueIterator<K, V> implements Iterator<V> {

    private final Iterator<Entry<K, V>> entryIterator;

    public ValueIterator(Iterator<Entry<K, V>> entryIterator) {
        this.entryIterator = entryIterator;
    }

    public boolean hasNext() {
        return entryIterator.hasNext();
    }

    public V next() {
        V next = entryIterator.next().getValue();
        if (next == null) {
            next = next();
        }
        return next;
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }
};
