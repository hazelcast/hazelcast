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

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

public interface CompactWriter {

    void writeInt(@Nonnull String fieldName, int value);

    void writeLong(@Nonnull String fieldName, long value);

    void writeString(@Nonnull String fieldName, String value);

    void writeBoolean(@Nonnull String fieldName, boolean value);

    void writeByte(@Nonnull String fieldName, byte value);

    void writeChar(@Nonnull String fieldName, char value);

    void writeDouble(@Nonnull String fieldName, double value);

    void writeFloat(@Nonnull String fieldName, float value);

    void writeShort(@Nonnull String fieldName, short value);

    void writeDecimal(@Nonnull String fieldName, BigDecimal value);

    void writeTime(@Nonnull String fieldName, LocalTime value);

    void writeDate(@Nonnull String fieldName, LocalDate value);

    void writeTimestamp(@Nonnull String fieldName, LocalDateTime value);

    void writeTimestampWithTimezone(@Nonnull String fieldName, OffsetDateTime value);

    void writeByteArray(@Nonnull String fieldName, byte[] value);

    void writeBooleanArray(@Nonnull String fieldName, boolean[] booleans);

    void writeCharArray(@Nonnull String fieldName, char[] value);

    void writeIntArray(@Nonnull String fieldName, int[] value);

    void writeLongArray(@Nonnull String fieldName, long[] value);

    void writeDoubleArray(@Nonnull String fieldName, double[] value);

    void writeFloatArray(@Nonnull String fieldName, float[] value);

    void writeShortArray(@Nonnull String fieldName, short[] value);

    void writeStringArray(@Nonnull String fieldName, String[] value);

    void writeDecimalArray(@Nonnull String fieldName, BigDecimal[] values);

    void writeTimeArray(@Nonnull String fieldName, LocalTime[] values);

    void writeDateArray(@Nonnull String fieldName, LocalDate[] values);

    void writeTimestampArray(@Nonnull String fieldName, LocalDateTime[] values);

    void writeTimestampWithTimezoneArray(@Nonnull String fieldName, OffsetDateTime[] values);

    <T> void writeObject(@Nonnull String fieldName, T value);

    <T> void writeObjectArray(@Nonnull String fieldName, T[] value);

}
