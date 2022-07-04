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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Supplier;

/**
 * Wraps a {@code Supplier} and returns a thread-safe memoizing supplier
 * which calls it only on the first invocation of {@code get()} and
 * afterwards returns the remembered instance.
 * <p>
 * The provided {@code Supplier} must return a non-null value.
 */
public final class ConcurrentMemoizingSupplier<T> implements Supplier<T> {
    private final Supplier<T> onceSupplier;
    private volatile T remembered;

    public ConcurrentMemoizingSupplier(Supplier<T> onceSupplier) {
        this.onceSupplier = onceSupplier;
    }

    @Override @Nonnull
    public T get() {
        // The common path will use a single volatile load
        T loadResult = remembered;
        if (loadResult != null) {
            return loadResult;
        }
        synchronized (this) {
            // The uncommon path can use simpler code with multiple volatile loads
            if (remembered != null) {
                return remembered;
            }
            remembered = onceSupplier.get();
            if (remembered == null) {
                throw new NullPointerException("Supplier returned null");
            }
            return remembered;
        }
    }

    /**
     * Get the remembered value. Returns {@code null} if value was never asked for
     */
    @Nullable
    public T remembered() {
        return remembered;
    }
}
