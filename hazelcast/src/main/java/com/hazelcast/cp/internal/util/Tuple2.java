/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.util;

import java.util.Objects;

/**
 * An immutable container of 2 statically typed fields
 * @param <X> type of the first element
 * @param <Y> type of the second element
 */
@SuppressWarnings("checkstyle:visibilitymodifier")
public final class Tuple2<X, Y> {

    public final X element1;
    public final Y element2;

    private Tuple2(X element1, Y element2) {
        this.element1 = element1;
        this.element2 = element2;
    }

    public static <X, Y> Tuple2<X, Y> of(X element1, Y element2) {
        return new Tuple2<>(element1, element2);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Tuple2<?, ?> tuple2 = (Tuple2<?, ?>) o;

        if (!Objects.equals(element1, tuple2.element1)) {
            return false;
        }
        return Objects.equals(element2, tuple2.element2);
    }

    @Override
    public int hashCode() {
        int result = element1 != null ? element1.hashCode() : 0;
        result = 31 * result + (element2 != null ? element2.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Tuple2{" + "element1=" + element1 + ", element2=" + element2 + '}';
    }
}
