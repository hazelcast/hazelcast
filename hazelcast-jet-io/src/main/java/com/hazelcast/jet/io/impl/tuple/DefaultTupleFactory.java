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

package com.hazelcast.jet.io.impl.tuple;

import com.hazelcast.jet.io.spi.tuple.Tuple;
import com.hazelcast.jet.io.spi.tuple.TupleFactory;

public class DefaultTupleFactory implements TupleFactory {
    @Override
    public <K, V> Tuple<K, V> tuple(K k, V v) {
        return new Tuple2<K, V>(k, v);
    }

    @Override
    public <K, V> Tuple<K, V> tuple(K k, V[] v) {
        return new DefaultTuple<K, V>(k, v);
    }

    @Override
    public <K, V> Tuple<K, V> tuple(K[] k, V[] v) {
        return new DefaultTuple<K, V>(k, v);
    }
}
