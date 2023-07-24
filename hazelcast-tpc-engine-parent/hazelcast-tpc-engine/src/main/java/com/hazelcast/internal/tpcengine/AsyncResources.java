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

package com.hazelcast.internal.tpcengine;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

/**
 * Contains a collection of resources like the AsyncSocket that are
 * specific to the Reactor.
 *
 * @param <E>
 */
public final class AsyncResources<E> {

    private final ConcurrentMap<E, E> map = new ConcurrentHashMap<>();

    public void add(E e) {
        map.put(e, e);
    }

    public void remove(E e) {
        map.remove(e);
    }

    public void foreach(Consumer<E> fn) {
        map.values().forEach(fn);
    }
}
