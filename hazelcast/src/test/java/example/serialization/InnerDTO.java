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

    InnerDTO() {
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    public InnerDTO(byte[] bb, char[] cc, short[] ss, int[] ii, long[] ll, float[] ff, double[] dd, NamedDTO[] nn,
                    BigDecimal[] bigDecimals, LocalTime[] localTimes, LocalDate[] localDates,
                    LocalDateTime[] localDateTimes, OffsetDateTime[] offsetDateTimes) {
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
                && Arrays.equals(offsetDateTimes, that.offsetDateTimes);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(bb);
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
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("InnerPortable{");
        sb.append("bb=").append(Arrays.toString(bb));
        sb.append(", cc=").append(Arrays.toString(cc));
        sb.append(", ss=").append(Arrays.toString(ss));
        sb.append(", ii=").append(Arrays.toString(ii));
        sb.append(", ll=").append(Arrays.toString(ll));
        sb.append(", ff=").append(Arrays.toString(ff));
        sb.append(", dd=").append(Arrays.toString(dd));
        sb.append(", nn=").append(Arrays.toString(nn));
        sb.append(", bigDecimals=").append(Arrays.toString(bigDecimals));
        sb.append(", localTimes=").append(Arrays.toString(localTimes));
        sb.append(", localDates=").append(Arrays.toString(localDates));
        sb.append(", localDateTimes=").append(Arrays.toString(localDateTimes));
        sb.append(", offsetDateTimes=").append(Arrays.toString(offsetDateTimes));
        sb.append('}');
        return sb.toString();
    }
}
