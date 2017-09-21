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

package com.hazelcast.jet.core;

import com.hazelcast.jet.Traverser;

import javax.annotation.Nullable;
import java.util.function.Consumer;

/**
 * Traverses over a single item which can be set from the outside, by using
 * this traverser as a {@code Consumer<T>}. Another item can be set at any
 * time and the subsequent {@code next()} call will consume it.
 *
 * @param <T> item type
 */
public class ResettableSingletonTraverser<T> implements Traverser<T>, Consumer<T> {
    private T item;

    @Override
    public T next() {
        try {
            return item;
        } finally {
            item = null;
        }
    }

    /**
     * Resets this traverser so that the following {@code next()} call
     * will return the item supplied here.
     *
     * @param item the item to return from {@code next()}
     */
    @Override
    public void accept(@Nullable T item) {
        assert this.item == null || item == null : "Previous item not emitted. Old=" + this.item + ", new=" + item;
        this.item = item;
    }
}
