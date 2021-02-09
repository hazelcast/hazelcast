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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryResultProducer;
import com.hazelcast.sql.impl.ResultIterator;
import com.hazelcast.sql.impl.row.Row;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class JetStaticQueryResultProducer implements QueryResultProducer {

    private final Iterator<? extends Row> iterator;

    private boolean iteratorRequested;

    public JetStaticQueryResultProducer(Iterator<? extends Row> iterator) {
        this.iterator = iterator;
    }

    @Override
    public ResultIterator<Row> iterator() {
        if (iteratorRequested) {
            throw new IllegalStateException("the iterator can be requested only once");
        }
        iteratorRequested = true;

        return new ResultIterator<Row>() {
            @Override
            public HasNextResult hasNext(long timeout, TimeUnit timeUnit) {
                return iterator.hasNext() ? HasNextResult.YES : HasNextResult.DONE;
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Row next() {
                return iterator.next();
            }
        };
    }

    @Override
    public void onError(QueryException error) {
    }
}
