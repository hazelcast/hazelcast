/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.util;

import com.hazelcast.jet.Traverser;

import java.util.function.Function;

/**
 * A flat-mapping decorator over a traverser.
 */
public class FlatMappingTraverser<T, R> implements Traverser<R> {
    private static final Traverser EMPTY_TRAVERSER = () -> null;

    private final Traverser<T> wrapped;
    private final Function<? super T, ? extends Traverser<? extends R>> mapper;
    private Traverser<? extends R> currentTraverser;

    public FlatMappingTraverser(Traverser<T> wrapped, Function<? super T, ? extends Traverser<? extends R>> mapper) {
        this.wrapped = wrapped;
        this.mapper = mapper;
        this.currentTraverser = nextTraverser();
    }

    @Override
    public R next() {
        do {
            R r = currentTraverser.next();
            if (r != null) {
                return r;
            }
            currentTraverser = nextTraverser();
        } while (currentTraverser != EMPTY_TRAVERSER);
        return null;
    }

    private Traverser<? extends R> nextTraverser() {
        final T t = wrapped.next();
        return t != null ? mapper.apply(t) : EMPTY_TRAVERSER;
    }
}
