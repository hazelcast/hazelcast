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

package com.hazelcast.jet.stream.impl.collectors;

import com.hazelcast.jet.stream.Distributed;

import java.util.Set;

public abstract class AbstractCollector<T, A, R> implements Distributed.Collector<T, A, R> {
    @Override
    public Distributed.Supplier<A> supplier() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Distributed.BiConsumer<A, T> accumulator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Distributed.BinaryOperator<A> combiner() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Distributed.Function<A, R> finisher() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Characteristics> characteristics() {
        throw new UnsupportedOperationException();
    }
}
