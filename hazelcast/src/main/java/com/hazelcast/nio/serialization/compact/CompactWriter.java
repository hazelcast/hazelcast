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

package com.hazelcast.nio.serialization.compact;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Collection;

public interface CompactWriter {

    void writeInt(String fieldName, int value);

    void writeLong(String fieldName, long value);

    void writeString(String fieldName, String value);

    void writeBoolean(String fieldName, boolean value);

    void writeByte(String fieldName, byte value);

    void writeChar(String fieldName, char value);

    void writeDouble(String fieldName, double value);

    void writeFloat(String fieldName, float value);

    void writeShort(String fieldName, short value);

    void writeDecimal(String fieldName, BigDecimal value);

    void writeTime(String fieldName, LocalTime value);

    void writeDate(String fieldName, LocalDate value);

    void writeTimestamp(String fieldName, LocalDateTime value);

    void writeTimestampWithTimezone(String fieldName, OffsetDateTime value);

    void writeByteArray(String fieldName, byte[] value);

    void writeBooleanArray(String fieldName, boolean[] booleans);

    void writeCharArray(String fieldName, char[] value);

    void writeIntArray(String fieldName, int[] value);

    void writeLongArray(String fieldName, long[] value);

    void writeDoubleArray(String fieldName, double[] value);

    void writeFloatArray(String fieldName, float[] value);

    void writeShortArray(String fieldName, short[] value);

    void writeStringArray(String fieldName, String[] value);

    void writeDecimalArray(String fieldName, BigDecimal[] values);

    void writeTimeArray(String fieldName, LocalTime[] values);

    void writeDateArray(String fieldName, LocalDate[] values);

    void writeTimestampArray(String fieldName, LocalDateTime[] values);

    void writeTimestampWithTimezoneArray(String fieldName, OffsetDateTime[] values);

    <T> void writeObject(String fieldName, T value);

    <T> void writeObjectArray(String fieldName, T[] value);

    <T> void writeObjectCollection(String fieldName, Collection<T> arrayList);

}
