/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.internal.impl.data.tuple;

import com.hazelcast.jet.api.data.tuple.JetTuple;
import com.hazelcast.jet.api.data.tuple.JetTupleConvertor;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.Iterator;

public class JetTupleIterator<R, K, V> implements Iterator<JetTuple<K, V>> {
    private final Iterator<R> iterator;
    private final JetTupleConvertor<R, K, V> convertor;
    private final SerializationService serializationService;

    public JetTupleIterator(Iterator<R> iterator,
                            JetTupleConvertor<R, K, V> convertor,
                            SerializationService serializationService) {
        this.iterator = iterator;
        this.convertor = convertor;
        this.serializationService = serializationService;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    public void remove() {
        throw new IllegalStateException("not supported exception");
    }

    @Override
    public JetTuple<K, V> next() {
        return convertor.convert(iterator.next(), serializationService);
    }
}
