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

package com.hazelcast.instance.impl.executejar;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Supplier;

/**
 * Creates the singleton on the first invocation of {@link #get(Supplier)}  and
 * afterwards returns the remembered instance.
 * <p>
 * The provided {@code Supplier} must return a non-null value.
 */
public final class ResettableSingleton<T> {
    private volatile T remembered;

    @Nonnull
    public T get(Supplier<T> onceSupplier) {
        // Double-checked locking to lazy initialize the singleton
        T loadResult = remembered;
        if (loadResult != null) {
            return loadResult;
        }
        synchronized (this) {
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

    /**
     * Reset the remembered value
     */
    public void resetRemembered() {
        synchronized (this) {
            remembered = null;
        }
    }
}
