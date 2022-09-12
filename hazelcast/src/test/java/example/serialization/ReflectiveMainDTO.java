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

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A class that includes all fields in the {@link MainDTO} class and some extra that are only
 * supported by the reflective serializer:
 * - Enum
 * - List
 * - Map
 * - Set
 * Used by {@link com.hazelcast.nio.serialization.compatibility.BinaryCompatibilityTest}.
 */
@SuppressWarnings({"checkstyle:ParameterNumber"})
public class ReflectiveMainDTO {

    public byte b;
    public boolean bool;
    public char c;
    public short s;
    public int i;
    public long l;
    public float f;
    public double d;
    public String str;
    public InnerDTO p;
    public BigDecimal bigDecimal;
    public LocalTime localTime;
    public LocalDate localDate;
    public LocalDateTime localDateTime;
    public OffsetDateTime offsetDateTime;
    public Byte nullableB;
    public Boolean nullableBool;
    public Character nullableC;
    public Short nullableS;
    public Integer nullableI;
    public Long nullableL;
    public Float nullableF;
    public Double nullableD;
    // Extra fields:
    public HiringStatus hiringStatus;
    public List<Integer> listOfNumbers;
    public Map<Integer, Integer> mapOfNumbers;
    public Set<Integer> setOfNumbers;

    ReflectiveMainDTO() {
    }

    public ReflectiveMainDTO(byte b, boolean bool, char c, short s, int i, long l, float f, double d, String str, InnerDTO p,
                             BigDecimal bigDecimal, LocalTime localTime, LocalDate localDate, LocalDateTime localDateTime,
                             OffsetDateTime offsetDateTime,
                             Byte nullableB, Boolean nullableBool, Character nullableC, Short nullableS, Integer nullableI,
                             Long nullableL, Float nullableF, Double nullableD, HiringStatus hiringStatus,
                             List<Integer> listOfNumbers, Map<Integer, Integer> mapOfNumbers, Set<Integer> setOfNumbers) {
        this.b = b;
        this.bool = bool;
        this.c = c;
        this.s = s;
        this.i = i;
        this.l = l;
        this.f = f;
        this.d = d;
        this.str = str;
        this.p = p;
        this.bigDecimal = bigDecimal;
        this.localTime = localTime;
        this.localDate = localDate;
        this.localDateTime = localDateTime;
        this.offsetDateTime = offsetDateTime;
        this.nullableB = nullableB;
        this.nullableBool = nullableBool;
        this.nullableC = nullableC;
        this.nullableS = nullableS;
        this.nullableI = nullableI;
        this.nullableL = nullableL;
        this.nullableF = nullableF;
        this.nullableD = nullableD;
        this.hiringStatus = hiringStatus;
        this.listOfNumbers = listOfNumbers;
        this.mapOfNumbers = mapOfNumbers;
        this.setOfNumbers = setOfNumbers;
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ReflectiveMainDTO reflectiveMainDTO = (ReflectiveMainDTO) o;

        if (b != reflectiveMainDTO.b) {
            return false;
        }
        if (bool != reflectiveMainDTO.bool) {
            return false;
        }
        if (c != reflectiveMainDTO.c) {
            return false;
        }
        if (s != reflectiveMainDTO.s) {
            return false;
        }
        if (i != reflectiveMainDTO.i) {
            return false;
        }
        if (l != reflectiveMainDTO.l) {
            return false;
        }
        if (Float.compare(reflectiveMainDTO.f, f) != 0) {
            return false;
        }
        if (Double.compare(reflectiveMainDTO.d, d) != 0) {
            return false;
        }
        if (!Objects.equals(str, reflectiveMainDTO.str)) {
            return false;
        }
        if (!Objects.equals(p, reflectiveMainDTO.p)) {
            return false;
        }
        if (!Objects.equals(bigDecimal, reflectiveMainDTO.bigDecimal)) {
            return false;
        }
        if (!Objects.equals(localTime, reflectiveMainDTO.localTime)) {
            return false;
        }
        if (!Objects.equals(localDate, reflectiveMainDTO.localDate)) {
            return false;
        }
        if (!Objects.equals(localDateTime, reflectiveMainDTO.localDateTime)) {
            return false;
        }
        if (!Objects.equals(offsetDateTime, reflectiveMainDTO.offsetDateTime)) {
            return false;
        }
        if (!Objects.equals(nullableB, reflectiveMainDTO.nullableB)) {
            return false;
        }
        if (!Objects.equals(nullableBool, reflectiveMainDTO.nullableBool)) {
            return false;
        }
        if (!Objects.equals(nullableC, reflectiveMainDTO.nullableC)) {
            return false;
        }
        if (!Objects.equals(nullableS, reflectiveMainDTO.nullableS)) {
            return false;
        }
        if (!Objects.equals(nullableI, reflectiveMainDTO.nullableI)) {
            return false;
        }
        if (!Objects.equals(nullableL, reflectiveMainDTO.nullableL)) {
            return false;
        }
        if (!Objects.equals(nullableF, reflectiveMainDTO.nullableF)) {
            return false;
        }
        if (!Objects.equals(nullableD, reflectiveMainDTO.nullableD)) {
            return false;
        }
        if (hiringStatus != reflectiveMainDTO.hiringStatus) {
            return false;
        }
        if (!Objects.equals(listOfNumbers, reflectiveMainDTO.listOfNumbers)) {
            return false;
        }
        if (!Objects.equals(mapOfNumbers, reflectiveMainDTO.mapOfNumbers)) {
            return false;
        }
        return Objects.equals(setOfNumbers, reflectiveMainDTO.setOfNumbers);
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = b;
        result = 31 * result + (bool ? 1 : 0);
        result = 31 * result + (int) c;
        result = 31 * result + (int) s;
        result = 31 * result + i;
        result = 31 * result + (int) (l ^ (l >>> 32));
        result = 31 * result + (f != 0.0f ? Float.floatToIntBits(f) : 0);
        temp = Double.doubleToLongBits(d);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (str != null ? str.hashCode() : 0);
        result = 31 * result + (p != null ? p.hashCode() : 0);
        result = 31 * result + (bigDecimal != null ? bigDecimal.hashCode() : 0);
        result = 31 * result + (localTime != null ? localTime.hashCode() : 0);
        result = 31 * result + (localDate != null ? localDate.hashCode() : 0);
        result = 31 * result + (localDateTime != null ? localDateTime.hashCode() : 0);
        result = 31 * result + (offsetDateTime != null ? offsetDateTime.hashCode() : 0);
        result = 31 * result + (nullableB != null ? nullableB.hashCode() : 0);
        result = 31 * result + (nullableBool != null ? nullableBool.hashCode() : 0);
        result = 31 * result + (nullableC != null ? nullableC.hashCode() : 0);
        result = 31 * result + (nullableS != null ? nullableS.hashCode() : 0);
        result = 31 * result + (nullableI != null ? nullableI.hashCode() : 0);
        result = 31 * result + (nullableL != null ? nullableL.hashCode() : 0);
        result = 31 * result + (nullableF != null ? nullableF.hashCode() : 0);
        result = 31 * result + (nullableD != null ? nullableD.hashCode() : 0);
        result = 31 * result + (hiringStatus != null ? hiringStatus.hashCode() : 0);
        result = 31 * result + (listOfNumbers != null ? listOfNumbers.hashCode() : 0);
        result = 31 * result + (mapOfNumbers != null ? mapOfNumbers.hashCode() : 0);
        result = 31 * result + (setOfNumbers != null ? setOfNumbers.hashCode() : 0);
        return result;
    }
}
