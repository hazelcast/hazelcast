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

package com.hazelcast.jet.datamodel;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

/**
 * An immutable 2-tuple (pair) of statically typed fields. Also implements
 * {@link Map.Entry}.
 *
 * @param <E0> the type of the field 0
 * @param <E1> the type of the field 1
 *
 * @since Jet 3.0
 */
public final class Tuple2<E0, E1> implements Map.Entry<E0, E1> {
    private final E0 f0;
    private final E1 f1;

    /**
     * Constructs a new 2-tuple with the supplied values.
     */
    private Tuple2(E0 f0, E1 f1) {
        this.f0 = f0;
        this.f1 = f1;
    }

    /**
     * Returns a new 2-tuple with the supplied values.
     */
    public static <E0, E1> Tuple2<E0, E1> tuple2(@Nullable E0 f0, @Nullable E1 f1) {
        return new Tuple2<>(f0, f1);
    }

    /**
     * Returns the value of the field 0.
     */
    @Nullable
    public E0 f0() {
        return f0;
    }

    /**
     * Returns the value of the field 1.
     */
    @Nullable
    public E1 f1() {
        return f1;
    }


    // Implementation of Map.Entry

    @Nullable @Override
    public E0 getKey() {
        return f0;
    }

    @Nullable @Override
    public E1 getValue() {
        return f1;
    }

    @Override
    public E1 setValue(@Nullable E1 value) {
        throw new UnsupportedOperationException("Tuple2 is immutable");
    }

    @Override
    @SuppressWarnings("rawtypes")
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Tuple2)) {
            return false;
        }
        final Tuple2 that = (Tuple2) obj;
        return Objects.equals(this.f0, that.f0)
                && Objects.equals(this.f1, that.f1);
    }

    @Override
    public int hashCode() {
        // This implementation is specified by Map.Entry and must not be changed
        return Objects.hashCode(f0) ^ Objects.hashCode(f1);
    }

    @Override
    public String toString() {
        return "(" + f0 + ", " + f1 + ')';
    }
}
