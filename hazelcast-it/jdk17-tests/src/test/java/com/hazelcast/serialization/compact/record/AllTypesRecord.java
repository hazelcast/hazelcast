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

package com.hazelcast.serialization.compact.record;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Objects;

public record AllTypesRecord(
        byte primitiveInt8,
        Byte objectInt8,
        short primitiveInt16,
        Short objectInt16,
        int primitiveInt32,
        Integer objectInt32,
        long primitiveInt64,
        Long objectInt64,
        float primitiveFloat32,
        Float objectFloat32,
        double primitiveFloat64,
        Double objectFloat64,
        boolean primitiveBoolean,
        Boolean objectBoolean,
        String string,
        BigDecimal decimal,
        LocalTime time,
        LocalDate date,
        LocalDateTime timestamp,
        OffsetDateTime timestampWithTimezone,
        AnEnum anEnum,
        NestedRecord nestedRecord,
        byte[] primitiveInt8Array,
        Byte[] objectInt8Array,
        short[] primitiveInt16Array,
        Short[] objectInt16Array,
        int[] primitiveInt32Array,
        Integer[] objectInt32Array,
        long[] primitiveInt64Array,
        Long[] objectInt64Array,
        float[] primitiveFloat32Array,
        Float[] objectFloat32Array,
        double[] primitiveFloat64Array,
        Double[] objectFloat64Array,
        boolean[] primitiveBooleanArray,
        Boolean[] objectBooleanArray,
        String[] stringArray,
        BigDecimal[] decimalArray,
        LocalTime[] timeArray,
        LocalDate[] dateArray,
        LocalDateTime[] timestampArray,
        OffsetDateTime[] timestampWithTimezoneArray,
        AnEnum[] anEnumArray,
        NestedRecord[] nestedRecordArray
) {

    enum AnEnum {
        VALUE0,
        VALUE1,
    }

    record NestedRecord(int primitiveInt32, String string) {
        public static NestedRecord create() {
            return new NestedRecord(42, "42");
        }
    }

    public static AllTypesRecord create() {
        return new AllTypesRecord(
                (byte) 42,
                (byte) -23,
                (short) 12345,
                (short) -12345,
                0,
                98237123,
                1321213321L,
                -891329819321123L,
                42.42F,
                -42.42F,
                9876.54321D,
                12345.6789D,
                true,
                Boolean.FALSE,
                "lorem ipsum",
                BigDecimal.valueOf(1234567.8901),
                LocalTime.now(),
                LocalDate.now(),
                LocalDateTime.now(),
                OffsetDateTime.now(),
                AnEnum.VALUE1,
                NestedRecord.create(),
                new byte[]{1, 2, -3},
                new Byte[]{42, Byte.MAX_VALUE, Byte.MIN_VALUE, null},
                new short[]{0},
                new Short[]{Short.MIN_VALUE, 0, null, Short.MIN_VALUE, Short.MAX_VALUE},
                new int[]{123, 456, 789, Integer.MIN_VALUE},
                new Integer[]{null, null, 0, Integer.MAX_VALUE},
                new long[]{123456789, -1},
                new Long[]{Long.MAX_VALUE, null, null},
                new float[]{42.3F, 3.42F},
                new Float[]{53.6F, -6.53F, null},
                new double[]{0},
                new Double[]{},
                new boolean[]{true, false, true},
                new Boolean[]{null, true, null, false},
                new String[]{"lorem", null, "ipsum"},
                new BigDecimal[]{BigDecimal.ONE, BigDecimal.ZERO},
                new LocalTime[]{LocalTime.now(), null},
                new LocalDate[]{LocalDate.now(), null},
                new LocalDateTime[]{null, LocalDateTime.now()},
                new OffsetDateTime[]{null, OffsetDateTime.now()},
                new AnEnum[]{AnEnum.VALUE0, null, AnEnum.VALUE1},
                new NestedRecord[]{null, NestedRecord.create()}
        );
    }

    public static AllTypesRecord createWithDefaultValues() {
        return new AllTypesRecord(
                (byte) 0,
                null,
                (short) 0,
                null,
                0,
                null,
                0,
                null,
                0,
                null,
                0,
                null,
                false,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AllTypesRecord that = (AllTypesRecord) o;
        return primitiveInt8 == that.primitiveInt8
                && primitiveInt16 == that.primitiveInt16
                && primitiveInt32 == that.primitiveInt32
                && primitiveInt64 == that.primitiveInt64
                && Float.compare(that.primitiveFloat32, primitiveFloat32) == 0
                && Double.compare(that.primitiveFloat64, primitiveFloat64) == 0
                && primitiveBoolean == that.primitiveBoolean
                && Objects.equals(objectInt8, that.objectInt8)
                && Objects.equals(objectInt16, that.objectInt16)
                && Objects.equals(objectInt32, that.objectInt32)
                && Objects.equals(objectInt64, that.objectInt64)
                && Objects.equals(objectFloat32, that.objectFloat32)
                && Objects.equals(objectFloat64, that.objectFloat64)
                && Objects.equals(objectBoolean, that.objectBoolean)
                && Objects.equals(string, that.string)
                && Objects.equals(decimal, that.decimal)
                && Objects.equals(time, that.time)
                && Objects.equals(date, that.date)
                && Objects.equals(timestamp, that.timestamp)
                && Objects.equals(timestampWithTimezone, that.timestampWithTimezone)
                && anEnum == that.anEnum
                && Objects.equals(nestedRecord, that.nestedRecord)
                && Arrays.equals(primitiveInt8Array, that.primitiveInt8Array)
                && Arrays.equals(objectInt8Array, that.objectInt8Array)
                && Arrays.equals(primitiveInt16Array, that.primitiveInt16Array)
                && Arrays.equals(objectInt16Array, that.objectInt16Array)
                && Arrays.equals(primitiveInt32Array, that.primitiveInt32Array)
                && Arrays.equals(objectInt32Array, that.objectInt32Array)
                && Arrays.equals(primitiveInt64Array, that.primitiveInt64Array)
                && Arrays.equals(objectInt64Array, that.objectInt64Array)
                && Arrays.equals(primitiveFloat32Array, that.primitiveFloat32Array)
                && Arrays.equals(objectFloat32Array, that.objectFloat32Array)
                && Arrays.equals(primitiveFloat64Array, that.primitiveFloat64Array)
                && Arrays.equals(objectFloat64Array, that.objectFloat64Array)
                && Arrays.equals(primitiveBooleanArray, that.primitiveBooleanArray)
                && Arrays.equals(objectBooleanArray, that.objectBooleanArray)
                && Arrays.equals(stringArray, that.stringArray)
                && Arrays.equals(decimalArray, that.decimalArray)
                && Arrays.equals(timeArray, that.timeArray)
                && Arrays.equals(dateArray, that.dateArray)
                && Arrays.equals(timestampArray, that.timestampArray)
                && Arrays.equals(timestampWithTimezoneArray, that.timestampWithTimezoneArray)
                && Arrays.equals(anEnumArray, that.anEnumArray)
                && Arrays.equals(nestedRecordArray, that.nestedRecordArray);
    }
}
