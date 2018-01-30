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

package com.hazelcast.jet.datamodel;

import java.util.Objects;

/**
 * An immutable 3-tuple (triple) of statically typed fields.
 *
 * @param <E0> the type of the field 0
 * @param <E1> the type of the field 1
 * @param <E2> the type of the field 2
 */
public final class Tuple3<E0, E1, E2> {
    private E0 f0;
    private E1 f1;
    private E2 f2;

    /**
     * Constructs a new 3-tuple with the supplied values.
     */
    private Tuple3(E0 f0, E1 f1, E2 f2) {
        this.f0 = f0;
        this.f1 = f1;
        this.f2 = f2;
    }

    /**
     * Returns a new 3-tuple with the supplied values.
     */
    public static <E0, E1, E2> Tuple3<E0, E1, E2> tuple3(E0 f0, E1 f1, E2 f2) {
        return new Tuple3<>(f0, f1, f2);
    }


    /**
     * Returns the value of the field 0.
     */
    public E0 f0() {
        return f0;
    }

    /**
     * Returns the value of the field 1.
     */
    public E1 f1() {
        return f1;
    }

    /**
     * Returns the value of the field 2.
     */
    public E2 f2() {
        return f2;
    }

    @Override
    public boolean equals(Object obj) {
        final Tuple3 that;
        return this == obj
                || obj instanceof Tuple3
                && Objects.equals(this.f0, (that = (Tuple3) obj).f0)
                && Objects.equals(this.f1, that.f1)
                && Objects.equals(this.f2, that.f2);
    }

    @Override
    public int hashCode() {
        int hc = 17;
        hc = 73 * hc + f0.hashCode();
        hc = 73 * hc + f1.hashCode();
        hc = 73 * hc + f2.hashCode();
        return hc;
    }

    @Override
    public String toString() {
        return "Tuple3{" + f0 + ", " + f1 + ", " + f2 + '}';
    }
}
