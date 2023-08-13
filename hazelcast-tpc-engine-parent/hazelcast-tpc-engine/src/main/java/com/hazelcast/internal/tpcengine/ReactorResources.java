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

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkPositive;

/**
 * Contains a collection of reactor resources like the AsyncSocket that are
 * specific to the Reactor.
 * <p/>
 * All methods on this class are thread-safe.
 *
 * @param <E> the type of the resource.
 */
public final class ReactorResources<E> {

    private final ConcurrentMap<E, E> map = new ConcurrentHashMap<>();
    private final int limit;

    public ReactorResources(int limit) {
        this.limit = checkPositive(limit, "limit");
    }

    /**
     * Adds a resource. If already added, call is ignored.
     *
     * @param e the resource to add.
     * @return true if added, false when rejected because the limit was exceeded.
     * @throws NullPointerException if e is null.
     */
    public boolean add(E e) {
        synchronized (this) {
            if (map.size() == limit) {
                return false;
            }

            map.put(e, e);
            return true;
        }
    }

    /**
     * Removes a resource. If already removed, call is ignored.
     *
     * @param e the resource to remove.
     * @throws NullPointerException if e is null.
     */
    public void remove(E e) {
        map.remove(e);
    }

    /**
     * Returns the size.
     *
     * @return the size.
     */
    public int size() {
        return map.size();
    }

    /**
     * Applies a fn on each resource.
     *
     * @param fn the function to apply.
     * @throws NullPointerException if fn is null.
     */
    public void foreach(Consumer<E> fn) {
        checkNotNull(fn, "fn");
        map.values().forEach(fn);
    }
}
