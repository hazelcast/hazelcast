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

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Arrays;

public class InnerDTO {

    public boolean[] bools;
    public byte[] bb;
    public short[] ss;
    public int[] ii;
    public long[] ll;
    public float[] ff;
    public double[] dd;
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
    public LocalTime[] nullableLocalTimes;
    public LocalDate[] nullableLocalDates;
    public LocalDateTime[] nullableLocalDateTimes;
    public OffsetDateTime[] nullableOffsetDateTimes;

    InnerDTO() {
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    public InnerDTO(boolean[] bools, byte[] bb, short[] ss, int[] ii, long[] ll, float[] ff, double[] dd, NamedDTO[] nn,
                    BigDecimal[] bigDecimals, LocalTime[] localTimes, LocalDate[] localDates,
                    LocalDateTime[] localDateTimes, OffsetDateTime[] offsetDateTimes,
                    Boolean[] nullableBools, Byte[] nullableBytes, Short[] nullableShorts, Integer[] nullableIntegers,
                    Long[] nullableLongs, Float[] nullableFloats, Double[] nullableDoubles, LocalTime[] nullableLocalTimes,
                    LocalDate[] nullableLocalDates, LocalDateTime[] nullableLocalDateTimes,
                    OffsetDateTime[] nullableOffsetDateTimes) {
        this.bools = bools;
        this.bb = bb;
        this.ss = ss;
        this.ii = ii;
        this.ll = ll;
        this.ff = ff;
        this.dd = dd;
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
        this.nullableLocalTimes = nullableLocalTimes;
        this.nullableLocalDates = nullableLocalDates;
        this.nullableLocalDateTimes = nullableLocalDateTimes;
        this.nullableOffsetDateTimes = nullableOffsetDateTimes;
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
        return Arrays.equals(bb, that.bb)
                && Arrays.equals(bools, that.bools)
                && Arrays.equals(ss, that.ss)
                && Arrays.equals(ii, that.ii)
                && Arrays.equals(ll, that.ll)
                && Arrays.equals(ff, that.ff)
                && Arrays.equals(dd, that.dd)
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
                && Arrays.equals(nullableDoubles, that.nullableDoubles)
                && Arrays.equals(nullableLocalTimes, that.nullableLocalTimes)
                && Arrays.equals(nullableLocalDates, that.nullableLocalDates)
                && Arrays.equals(nullableLocalDateTimes, that.nullableLocalDateTimes)
                && Arrays.equals(nullableOffsetDateTimes, that.nullableOffsetDateTimes);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(bb);
        result = 31 * result + Arrays.hashCode(bools);
        result = 31 * result + Arrays.hashCode(ss);
        result = 31 * result + Arrays.hashCode(ii);
        result = 31 * result + Arrays.hashCode(ll);
        result = 31 * result + Arrays.hashCode(ff);
        result = 31 * result + Arrays.hashCode(dd);
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
        result = 31 * result + Arrays.hashCode(nullableLocalTimes);
        result = 31 * result + Arrays.hashCode(nullableLocalDates);
        result = 31 * result + Arrays.hashCode(nullableLocalDateTimes);
        result = 31 * result + Arrays.hashCode(nullableOffsetDateTimes);
        return result;
    }

    @Override
    public String toString() {
        return "InnerDTO{"
                + "+ bools=" + Arrays.toString(bools)
                + "+ bb=" + Arrays.toString(bb)
                + ", + ss=" + Arrays.toString(ss)
                + ", + ii=" + Arrays.toString(ii)
                + ", + ll=" + Arrays.toString(ll)
                + ", + ff=" + Arrays.toString(ff)
                + ", + dd=" + Arrays.toString(dd)
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
                + ", + nullableLocalTimes=" + Arrays.toString(nullableLocalTimes)
                + ", + nullableLocalDates=" + Arrays.toString(nullableLocalDates)
                + ", + nullableLocalDateTimes=" + Arrays.toString(nullableLocalDateTimes)
                + ", + nullableOffsetDateTimes=" + Arrays.toString(nullableOffsetDateTimes)
                + '}';
    }
}
