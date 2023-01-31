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

import java.util.Arrays;

public class ArrayOfFixedSizeFieldsDTO {

    public byte[] b;
    public boolean[] bool;
    public short[] s;
    public int[] i;
    public long[] l;
    public float[] f;
    public double[] d;

    ArrayOfFixedSizeFieldsDTO() {
    }

    public ArrayOfFixedSizeFieldsDTO(byte[] b, boolean[] bool, short[] s, int[] i, long[] l, float[] f, double[] d) {
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
        ArrayOfFixedSizeFieldsDTO that = (ArrayOfFixedSizeFieldsDTO) o;
        return Arrays.equals(b, that.b) && Arrays.equals(bool, that.bool) && Arrays.equals(s, that.s)
                && Arrays.equals(i, that.i) && Arrays.equals(l, that.l) && Arrays.equals(f, that.f)
                && Arrays.equals(d, that.d);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(b);
        result = 31 * result + Arrays.hashCode(bool);
        result = 31 * result + Arrays.hashCode(s);
        result = 31 * result + Arrays.hashCode(i);
        result = 31 * result + Arrays.hashCode(l);
        result = 31 * result + Arrays.hashCode(f);
        result = 31 * result + Arrays.hashCode(d);
        return result;
    }
}
