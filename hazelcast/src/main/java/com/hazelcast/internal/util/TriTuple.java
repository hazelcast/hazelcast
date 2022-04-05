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

package com.hazelcast.internal.util;

import java.util.Objects;

/**
 * An immutable container of 3 statically typed fields
 *
 * @param <X> type of the first element
 * @param <Y> type of the second element
 * @param <Z> type of the third element
 */
@SuppressWarnings("checkstyle:visibilitymodifier")
public final class TriTuple<X, Y, Z> {

    public final X element1;
    public final Y element2;
    public final Z element3;

    private TriTuple(X element1, Y element2, Z element3) {
        this.element1 = element1;
        this.element2 = element2;
        this.element3 = element3;
    }

    public static <X, Y, Z> TriTuple<X, Y, Z> of(X element1, Y element2, Z element3) {
        return new TriTuple<>(element1, element2, element3);
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TriTuple<?, ?, ?> triTuple = (TriTuple<?, ?, ?>) o;

        if (!Objects.equals(element1, triTuple.element1)) {
            return false;
        }
        if (!Objects.equals(element2, triTuple.element2)) {
            return false;
        }
        return Objects.equals(element3, triTuple.element3);
    }

    @Override
    public int hashCode() {
        int result = element1 != null ? element1.hashCode() : 0;
        result = 31 * result + (element2 != null ? element2.hashCode() : 0);
        result = 31 * result + (element3 != null ? element3.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TriTuple{" + "element1=" + element1 + ", element2=" + element2 + ", element3=" + element3 + '}';
    }
}
