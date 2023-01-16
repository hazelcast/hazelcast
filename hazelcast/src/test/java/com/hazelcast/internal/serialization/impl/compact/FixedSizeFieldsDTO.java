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
package com.hazelcast.internal.serialization.impl.compact;

import java.util.Objects;

public class FixedSizeFieldsDTO {

    public byte b;
    public boolean bool;
    public short s;
    public int i;
    public long l;
    public float f;
    public double d;

    FixedSizeFieldsDTO() {
    }

    public FixedSizeFieldsDTO(byte b, boolean bool, short s, int i, long l, float f, double d) {
        this.b = b;
        this.bool = bool;
        this.s = s;
        this.i = i;
        this.l = l;
        this.f = f;
        this.d = d;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FixedSizeFieldsDTO that = (FixedSizeFieldsDTO) o;
        return b == that.b && bool == that.bool && s == that.s && i == that.i && l == that.l
                && Float.compare(that.f, f) == 0 && Double.compare(that.d, d) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(b, bool, s, i, l, f, d);
    }

    @Override
    public String toString() {
        return "FixedSizeFieldsDTO{"
                + "b=" + b
                + ", bool=" + bool
                + ", s=" + s
                + ", i=" + i
                + ", l=" + l
                + ", f=" + f
                + ", d=" + d
                + '}';
    }
}
