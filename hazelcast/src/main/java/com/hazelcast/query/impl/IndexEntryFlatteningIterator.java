/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.AbstractCompositeIterator;

import java.util.Iterator;
import java.util.Map;

@SuppressWarnings("rawtypes")
final class IndexEntryFlatteningIterator extends AbstractCompositeIterator<QueryableEntry> {

    private final Iterator<Map<Data, QueryableEntry>> iterator;

    IndexEntryFlatteningIterator(Iterator<Map<Data, QueryableEntry>> iterator) {
        this.iterator = iterator;
    }

    @Override
    protected Iterator<QueryableEntry> nextIterator() {
        while (iterator.hasNext()) {
            Map<Data, QueryableEntry> map = iterator.next();

            Iterator<QueryableEntry> currentIterator0 = map.values().iterator();

            if (currentIterator0.hasNext()) {
                return currentIterator0;
            }
        }

        return null;
    }
}
