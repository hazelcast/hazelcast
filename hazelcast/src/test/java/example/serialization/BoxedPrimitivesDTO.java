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

import java.util.Objects;

public class BoxedPrimitivesDTO {
    private Byte b1;
    private Character c1;
    private Float f1;
    private Double d1;
    private Boolean bool1;
    private Long l1;
    private Short s1;
    private Integer i1;

    public BoxedPrimitivesDTO() {
    }

    public BoxedPrimitivesDTO(
      Byte b1,
      Character c1,
      Float f1,
      Double d1,
      Boolean bool1,
      Long l1,
      Short s1,
      Integer i1) {
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
        BoxedPrimitivesDTO that = (BoxedPrimitivesDTO) o;
        return Objects.equals(b1, that.b1)
          && Objects.equals(c1, that.c1)
          && Objects.equals(f1, that.f1)
          && Objects.equals(d1, that.d1)
          && Objects.equals(bool1, that.bool1)
          && Objects.equals(l1, that.l1)
          && Objects.equals(s1, that.s1)
          && Objects.equals(i1, that.i1);
    }

    @Override
    public int hashCode() {
        return Objects.hash(b1, c1, f1, d1, bool1, l1, s1, i1);
    }
}
