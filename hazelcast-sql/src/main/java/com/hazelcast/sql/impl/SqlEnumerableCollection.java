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

package com.hazelcast.sql.impl;

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;

import java.util.Collection;
import java.util.Iterator;

public class SqlEnumerableCollection<T> extends AbstractEnumerable<T> {

    private final Collection<T> collection;

    public SqlEnumerableCollection(Collection<T> collection) {
        this.collection = collection;
    }

    @Override
    public Enumerator<T> enumerator() {
        return new EnumeratorImpl();
    }

    private class EnumeratorImpl implements Enumerator<T> {

        private Iterator<T> iterator = collection.iterator();

        private T current;

        @Override
        public T current() {
            return current;
        }

        @Override
        public boolean moveNext() {
            if (!iterator.hasNext()) {
                return false;
            }

            current = iterator.next();

            return true;
        }

        @Override
        public void reset() {
            iterator = collection.iterator();
        }

        @Override
        public void close() {
        }

    }

}
