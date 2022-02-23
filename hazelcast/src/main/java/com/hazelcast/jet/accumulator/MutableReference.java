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

package com.hazelcast.jet.accumulator;

import java.util.Objects;

/**
 * Mutable container of an object reference.
 *
 * @param <T> referenced object type
 *
 * @since Jet 3.0
 */
public class MutableReference<T> {

    private T value;

    /**
     * Creates a new instance with a {@code null} value.
     */
    public MutableReference() {
    }

    /**
     * Creates a new instance with the specified value.
     */
    public MutableReference(T value) {
        this.value = value;
    }

    /**
     * Returns the current value.
     */
    public T get() {
        return value;
    }

    /**
     * Tells whether the current value is {@code null}.
     */
    public boolean isNull() {
        return value == null;
    }

    /**
     * Sets the value as given.
     */
    public MutableReference set(T value) {
        this.value = value;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        return this == o ||
                o != null
                && this.getClass() == o.getClass()
                && Objects.equals(this.value, ((MutableReference) o).value);
    }

    @Override
    public int hashCode() {
        return value != null ? value.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "MutableReference(" + value + ')';
    }
}
