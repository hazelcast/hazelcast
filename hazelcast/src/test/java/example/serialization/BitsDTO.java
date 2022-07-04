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

package example.serialization;

import java.util.Arrays;
import java.util.Objects;

public class BitsDTO {

    public boolean a;
    public boolean b;
    public boolean c;
    public boolean d;
    public boolean e;
    public boolean f;
    public boolean g;
    public boolean h;
    public int id;
    public boolean[] booleans;

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BitsDTO bitsDTO = (BitsDTO) o;
        return a == bitsDTO.a && b == bitsDTO.b && c == bitsDTO.c
                && d == bitsDTO.d && e == bitsDTO.e && f == bitsDTO.f && g == bitsDTO.g
                && h == bitsDTO.h && id == bitsDTO.id && Arrays.equals(booleans, bitsDTO.booleans);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(a, b, c, d, e, f, g, h, id);
        result = 31 * result + Arrays.hashCode(booleans);
        return result;
    }

    @Override
    public String toString() {
        return "BitsDTO{"
                + "a=" + a + ", b=" + b + ", c=" + c + ", d=" + d + ", e=" + e + ", f=" + f + ", g=" + g
                + ", h=" + h + ", id=" + id + ", booleans=" + Arrays.toString(booleans) + '}';
    }
}
