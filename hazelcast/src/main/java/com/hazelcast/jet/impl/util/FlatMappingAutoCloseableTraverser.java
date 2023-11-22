/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.impl.AutoCloseableTraversers;
import com.hazelcast.jet.Traverser;

import java.util.function.Function;

/**
 * Same as {@link FlatMappingTraverser}. This traverser will close the current traverser
 */
public class FlatMappingAutoCloseableTraverser<T, R> implements AutoCloseableTraverser<R> {

    // Do not replace with lambda as we rely on the NULL_TRAVERSER to be our
    // own unique instance, which is not guaranteed with lambda.
    @SuppressWarnings("Convert2Lambda")
    private static final AutoCloseableTraverser NULL_TRAVERSER = new AutoCloseableTraverser() {
        @Override
        public Object next() {
            return null;
        }
    };

    private final Traverser<T> wrapped;
    private final Function<? super T, ? extends AutoCloseableTraverser<? extends R>> mapper;
    private AutoCloseableTraverser<? extends R> currentTraverser = AutoCloseableTraversers.emptyAutoCloseableTraverser();

    public FlatMappingAutoCloseableTraverser(Traverser<T> wrapped,
                                             Function<? super T, ? extends AutoCloseableTraverser<? extends R>> mapper) {
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
    private AutoCloseableTraverser<? extends R> nextTraverser() {
        final T t = wrapped.next();
        return t != null ? mapper.apply(t) : NULL_TRAVERSER;
    }

    @Override
    public void close() throws Exception {
        if (currentTraverser != NULL_TRAVERSER) {
            currentTraverser.close();
        }
    }
}
