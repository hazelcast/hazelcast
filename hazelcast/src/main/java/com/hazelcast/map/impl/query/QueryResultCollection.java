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

package com.hazelcast.map.impl.query;

import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.IterationType;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

public class QueryResultCollection<E> extends AbstractSet<E> {

    private final Collection<QueryResultRow> rows;
    private final SerializationService serializationService;
    private final IterationType iterationType;
    private final boolean binary;

    public QueryResultCollection(SerializationService serializationService, IterationType iterationType, boolean binary,
                                 boolean unique) {
        this.serializationService = serializationService;
        this.iterationType = iterationType;
        this.binary = binary;
        if (unique) {
            rows = new HashSet<QueryResultRow>();
        } else {
            rows = new ArrayList<QueryResultRow>();
        }
    }

    public QueryResultCollection(SerializationService serializationService, IterationType iterationType, boolean binary,
                                 boolean unique, QueryResult queryResult) {

        this(serializationService, iterationType, binary, unique);
        addAllRows(queryResult.getRows());
    }

    // just for testing
    Collection<QueryResultRow> getRows() {
        return rows;
    }

    public IterationType getIterationType() {
        return iterationType;
    }

    public void addAllRows(Collection<QueryResultRow> collection) {
        rows.addAll(collection);
    }

    @Override
    public Iterator<E> iterator() {
        return new QueryResultIterator(rows.iterator(), iterationType, binary, serializationService);
    }

    @Override
    public int size() {
        return rows.size();
    }
}
