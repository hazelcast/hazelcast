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
    public char[] cc;
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
    public Boolean[] nullablebools;
    public Byte[] nullablebb;
    public Short[] nullabless;
    public Integer[] nullableii;
    public Long[] nullablell;
    public Float[] nullableff;
    public Double[] nullabledd;
    public LocalTime[] nullablelocalTimes;
    public LocalDate[] nullablelocalDates;
    public LocalDateTime[] nullablelocalDateTimes;
    public OffsetDateTime[] nullableoffsetDateTimes;

    InnerDTO() {
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    public InnerDTO(boolean[] bools, byte[] bb, char[] cc, short[] ss, int[] ii, long[] ll, float[] ff, double[] dd, NamedDTO[] nn,
                    BigDecimal[] bigDecimals, LocalTime[] localTimes, LocalDate[] localDates,
                    LocalDateTime[] localDateTimes, OffsetDateTime[] offsetDateTimes,
                    Boolean[] nullablebools, Byte[] nullablebb, Short[] nullabless, Integer[] nullableii,
                    Long[] nullablell, Float[] nullableff, Double[] nullabledd, LocalTime[] nullablelocalTimes,
                    LocalDate[] nullablelocalDates, LocalDateTime[] nullablelocalDateTimes,
                    OffsetDateTime[] nullableoffsetDateTimes) {
        this.bools = bools;
        this.bb = bb;
        this.cc = cc;
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
        this.nullablebools = nullablebools;
        this.nullablebb = nullablebb;
        this.nullabless = nullabless;
        this.nullableii = nullableii;
        this.nullablell = nullablell;
        this.nullableff = nullableff;
        this.nullabledd = nullabledd;
        this.nullablelocalTimes = nullablelocalTimes;
        this.nullablelocalDates = nullablelocalDates;
        this.nullablelocalDateTimes = nullablelocalDateTimes;
        this.nullableoffsetDateTimes = nullableoffsetDateTimes;
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
                && Arrays.equals(cc, that.cc)
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
                && Arrays.equals(nullablebools, that.nullablebools)
                && Arrays.equals(nullabless, that.nullabless)
                && Arrays.equals(nullableii, that.nullableii)
                && Arrays.equals(nullablell, that.nullablell)
                && Arrays.equals(nullableff, that.nullableff)
                && Arrays.equals(nullabledd, that.nullabledd)
                && Arrays.equals(nullablelocalTimes, that.nullablelocalTimes)
                && Arrays.equals(nullablelocalDates, that.nullablelocalDates)
                && Arrays.equals(nullablelocalDateTimes, that.nullablelocalDateTimes)
                && Arrays.equals(nullableoffsetDateTimes, that.nullableoffsetDateTimes);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(bb);
        result = 31 * result + Arrays.hashCode(bools);
        result = 31 * result + Arrays.hashCode(cc);
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
        result = 31 * result + Arrays.hashCode(nullablebools);
        result = 31 * result + Arrays.hashCode(nullablebb);
        result = 31 * result + Arrays.hashCode(nullabless);
        result = 31 * result + Arrays.hashCode(nullableii);
        result = 31 * result + Arrays.hashCode(nullablell);
        result = 31 * result + Arrays.hashCode(nullableff);
        result = 31 * result + Arrays.hashCode(nullabledd);
        result = 31 * result + Arrays.hashCode(nullablelocalTimes);
        result = 31 * result + Arrays.hashCode(nullablelocalDates);
        result = 31 * result + Arrays.hashCode(nullablelocalDateTimes);
        result = 31 * result + Arrays.hashCode(nullableoffsetDateTimes);
        return result;
    }

    @Override
    public String toString() {
        return "InnerDTO{"
                + "+ bools=" + Arrays.toString(bools)
                + "+ bb=" + Arrays.toString(bb)
                + ", + cc=" + Arrays.toString(cc)
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
                + ", + nullablebools=" + Arrays.toString(nullablebools)
                + ", + nullablebb=" + Arrays.toString(nullablebb)
                + ", + nullabless=" + Arrays.toString(nullabless)
                + ", + nullableii=" + Arrays.toString(nullableii)
                + ", + nullablell=" + Arrays.toString(nullablell)
                + ", + nullableff=" + Arrays.toString(nullableff)
                + ", + nullabledd=" + Arrays.toString(nullabledd)
                + ", + nullablelocalTimes=" + Arrays.toString(nullablelocalTimes)
                + ", + nullablelocalDates=" + Arrays.toString(nullablelocalDates)
                + ", + nullablelocalDateTimes=" + Arrays.toString(nullablelocalDateTimes)
                + ", + nullableoffsetDateTimes=" + Arrays.toString(nullableoffsetDateTimes)
                + '}';
    }
}
