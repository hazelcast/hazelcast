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

import com.hazelcast.client.impl.Values;
import com.hazelcast.core.Instance;

import java.util.Iterator;
import java.util.Map;

public class MultiMapEntryIterator extends MapEntryIterator {
    private volatile Iterator currentValueIterator;
    private volatile Object lastKey;
    protected volatile Map.Entry lastEntry;

    public MultiMapEntryIterator(Iterator it, EntryHolder proxy, Instance.InstanceType instanceType) {
        super(it, proxy, instanceType);
    }

    public boolean hasNext() {
        if (currentValueIterator == null) {
            return it.hasNext();
        }
        return currentValueIterator.hasNext() || it.hasNext();
    }

    public Map.Entry next() {
        if (currentValueIterator == null || !currentValueIterator.hasNext()) {
            lastKey = it.next();
            Values value = (Values) proxy.get(lastKey);
            if (value == null) {
                return next();
            }
            currentValueIterator = value.iterator();
        }
        if (currentValueIterator == null || !currentValueIterator.hasNext()) {
            return next();
        }
        lastEntry = new MapEntry(lastKey, currentValueIterator.next(), proxy);
        return lastEntry;
    }
}
