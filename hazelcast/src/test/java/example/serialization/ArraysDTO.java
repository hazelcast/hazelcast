/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

public class ArraysDTO {
    private byte[] b1;
    private char[] c1;
    private float[] f1;
    private double[] d1;
    private boolean[] bool1;
    private long[] l1;
    private short[] s1;
    private int[] i1;

    public ArraysDTO() {
    }

    public ArraysDTO(byte[] b1,
              char[] c1,
              float[] f1,
              double[] d1,
              boolean[] bool1,
              long[] l1,
              short[] s1,
              int[] i1) {
        this.b1 = b1;
        this.c1 = c1;
        this.f1 = f1;
        this.d1 = d1;
        this.bool1 = bool1;
        this.l1 = l1;
        this.s1 = s1;
        this.i1 = i1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ArraysDTO arraysDTO = (ArraysDTO) o;
        return Arrays.equals(b1, arraysDTO.b1)
          && Arrays.equals(c1, arraysDTO.c1)
          && Arrays.equals(f1, arraysDTO.f1)
          && Arrays.equals(d1, arraysDTO.d1)
          && Arrays.equals(bool1, arraysDTO.bool1)
          && Arrays.equals(l1, arraysDTO.l1)
          && Arrays.equals(s1, arraysDTO.s1)
          && Arrays.equals(i1, arraysDTO.i1);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(b1);
        result = 31 * result + Arrays.hashCode(c1);
        result = 31 * result + Arrays.hashCode(f1);
        result = 31 * result + Arrays.hashCode(d1);
        result = 31 * result + Arrays.hashCode(bool1);
        result = 31 * result + Arrays.hashCode(l1);
        result = 31 * result + Arrays.hashCode(s1);
        result = 31 * result + Arrays.hashCode(i1);
        return result;
    }
}
