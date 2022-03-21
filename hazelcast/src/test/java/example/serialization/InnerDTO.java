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
import java.util.Arrays;

public class InnerDTO {

    public boolean[] bools;
    public byte[] bytes;
    public short[] shorts;
    public int[] ints;
    public long[] longs;
    public float[] floats;
    public double[] doubles;
    public String[] strings;
    public NamedDTO[] nn;
    public BigDecimal[] bigDecimals;
    public LocalTime[] localTimes;
    public LocalDate[] localDates;
    public LocalDateTime[] localDateTimes;
    public OffsetDateTime[] offsetDateTimes;
    public Boolean[] nullableBools;
    public Byte[] nullableBytes;
    public Short[] nullableShorts;
    public Integer[] nullableIntegers;
    public Long[] nullableLongs;
    public Float[] nullableFloats;
    public Double[] nullableDoubles;

    InnerDTO() {
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    public InnerDTO(boolean[] bools, byte[] bb, short[] ss, int[] ii, long[] ll, float[] ff, double[] dd,
                    String[] strings, NamedDTO[] nn,
                    BigDecimal[] bigDecimals, LocalTime[] localTimes, LocalDate[] localDates,
                    LocalDateTime[] localDateTimes, OffsetDateTime[] offsetDateTimes,
                    Boolean[] nullableBools, Byte[] nullableBytes, Short[] nullableShorts, Integer[] nullableIntegers,
                    Long[] nullableLongs, Float[] nullableFloats, Double[] nullableDoubles) {
        this.bools = bools;
        this.bytes = bb;
        this.shorts = ss;
        this.ints = ii;
        this.longs = ll;
        this.floats = ff;
        this.doubles = dd;
        this.strings = strings;
        this.nn = nn;
        this.bigDecimals = bigDecimals;
        this.localTimes = localTimes;
        this.localDates = localDates;
        this.localDateTimes = localDateTimes;
        this.offsetDateTimes = offsetDateTimes;
        this.nullableBools = nullableBools;
        this.nullableBytes = nullableBytes;
        this.nullableShorts = nullableShorts;
        this.nullableIntegers = nullableIntegers;
        this.nullableLongs = nullableLongs;
        this.nullableFloats = nullableFloats;
        this.nullableDoubles = nullableDoubles;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InnerDTO that = (InnerDTO) o;
        return Arrays.equals(bytes, that.bytes)
                && Arrays.equals(bools, that.bools)
                && Arrays.equals(shorts, that.shorts)
                && Arrays.equals(ints, that.ints)
                && Arrays.equals(longs, that.longs)
                && Arrays.equals(floats, that.floats)
                && Arrays.equals(doubles, that.doubles)
                && Arrays.equals(strings, that.strings)
                && Arrays.equals(nn, that.nn)
                && Arrays.equals(bigDecimals, that.bigDecimals)
                && Arrays.equals(localTimes, that.localTimes)
                && Arrays.equals(localDates, that.localDates)
                && Arrays.equals(localDateTimes, that.localDateTimes)
                && Arrays.equals(offsetDateTimes, that.offsetDateTimes)
                && Arrays.equals(nullableBools, that.nullableBools)
                && Arrays.equals(nullableShorts, that.nullableShorts)
                && Arrays.equals(nullableIntegers, that.nullableIntegers)
                && Arrays.equals(nullableLongs, that.nullableLongs)
                && Arrays.equals(nullableFloats, that.nullableFloats)
                && Arrays.equals(nullableDoubles, that.nullableDoubles);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(bytes);
        result = 31 * result + Arrays.hashCode(bools);
        result = 31 * result + Arrays.hashCode(shorts);
        result = 31 * result + Arrays.hashCode(ints);
        result = 31 * result + Arrays.hashCode(longs);
        result = 31 * result + Arrays.hashCode(floats);
        result = 31 * result + Arrays.hashCode(doubles);
        result = 31 * result + Arrays.hashCode(strings);
        result = 31 * result + Arrays.hashCode(nn);
        result = 31 * result + Arrays.hashCode(bigDecimals);
        result = 31 * result + Arrays.hashCode(localTimes);
        result = 31 * result + Arrays.hashCode(localDates);
        result = 31 * result + Arrays.hashCode(localDateTimes);
        result = 31 * result + Arrays.hashCode(offsetDateTimes);
        result = 31 * result + Arrays.hashCode(nullableBools);
        result = 31 * result + Arrays.hashCode(nullableBytes);
        result = 31 * result + Arrays.hashCode(nullableShorts);
        result = 31 * result + Arrays.hashCode(nullableIntegers);
        result = 31 * result + Arrays.hashCode(nullableLongs);
        result = 31 * result + Arrays.hashCode(nullableFloats);
        result = 31 * result + Arrays.hashCode(nullableDoubles);
        return result;
    }

    @Override
    public String toString() {
        return "InnerDTO{"
                + "+ bools=" + Arrays.toString(bools)
                + "+ bb=" + Arrays.toString(bytes)
                + ", + ss=" + Arrays.toString(shorts)
                + ", + ii=" + Arrays.toString(ints)
                + ", + ll=" + Arrays.toString(longs)
                + ", + ff=" + Arrays.toString(floats)
                + ", + dd=" + Arrays.toString(doubles)
                + ", + strings=" + Arrays.toString(strings)
                + ", + nn=" + Arrays.toString(nn)
                + ", + bigDecimals=" + Arrays.toString(bigDecimals)
                + ", + localTimes=" + Arrays.toString(localTimes)
                + ", + localDates=" + Arrays.toString(localDates)
                + ", + localDateTimes=" + Arrays.toString(localDateTimes)
                + ", + offsetDateTimes=" + Arrays.toString(offsetDateTimes)
                + ", + nullableBools=" + Arrays.toString(nullableBools)
                + ", + nullableBytes=" + Arrays.toString(nullableBytes)
                + ", + nullableShorts=" + Arrays.toString(nullableShorts)
                + ", + nullableIntegers=" + Arrays.toString(nullableIntegers)
                + ", + nullableLongs=" + Arrays.toString(nullableLongs)
                + ", + nullableFloats=" + Arrays.toString(nullableFloats)
                + ", + nullableDoubles=" + Arrays.toString(nullableDoubles)
                + '}';
    }
}
