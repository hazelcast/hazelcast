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

package com.hazelcast.jet.impl.util;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;

import java.util.function.Function;

/**
 * A flat-mapping decorator over a traverser.
 */
public class FlatMappingTraverser<T, R> implements Traverser<R> {

    // Do not replace with lambda as we rely on the NULL_TRAVERSER to be our
    // own unique instance, which is not guaranteed with lambda.
    @SuppressWarnings("Convert2Lambda")
    private static final Traverser NULL_TRAVERSER = new Traverser() {
        @Override
        public Object next() {
            return null;
        }
    };

    private final Traverser<T> wrapped;
    private final Function<? super T, ? extends Traverser<? extends R>> mapper;
    private Traverser<? extends R> currentTraverser = Traversers.empty();

    public FlatMappingTraverser(Traverser<T> wrapped, Function<? super T, ? extends Traverser<? extends R>> mapper) {
        this.wrapped = wrapped;
        this.mapper = mapper;
    }

    @Override
    public R next() {
        do {
            R r = currentTraverser.next();
            if (r != null) {
                return r;
            }
            currentTraverser = nextTraverser();
        } while (currentTraverser != NULL_TRAVERSER);
        return null;
    }

    @SuppressWarnings("unchecked")
    private Traverser<? extends R> nextTraverser() {
        final T t = wrapped.next();
        return t != null ? mapper.apply(t) : NULL_TRAVERSER;
    }
}
