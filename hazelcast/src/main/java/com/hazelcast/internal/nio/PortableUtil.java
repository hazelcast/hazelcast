/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.nio;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.time.LocalTime;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;


public final class PortableUtil {

    private PortableUtil() {
    }

    public static void writeLocalTime(ObjectDataOutput out, LocalTime value) throws IOException {
        IOUtil.writeLocalTime(out, value);
    }

    public static LocalTime readLocalTime(ObjectDataInput in) throws IOException {
        return IOUtil.readLocalTime(in);
    }

    public static void writeLocalDate(ObjectDataOutput out, LocalDate value) throws IOException {
        // a short used here for backward compatibility, other places use an int
        int year = value.getYear();
        int monthValue = value.getMonthValue();
        int dayOfMonth = value.getDayOfMonth();
        out.writeShort(year);
        out.writeByte(monthValue);
        out.writeByte(dayOfMonth);
    }

    public static LocalDate readLocalDate(ObjectDataInput in) throws IOException {
        // a short used here for backward compatibility, other places use an int
        int year = in.readShort();
        int month = in.readByte();
        int dayOfMonth = in.readByte();
        return LocalDate.of(year, month, dayOfMonth);
    }

    public static void writeLocalDateTime(ObjectDataOutput out, LocalDateTime value) throws IOException {
        writeLocalDate(out, value.toLocalDate());
        writeLocalTime(out, value.toLocalTime());
    }

    public static LocalDateTime readLocalDateTime(ObjectDataInput in) throws IOException {
        LocalDate date = readLocalDate(in);
        LocalTime time = readLocalTime(in);

        return LocalDateTime.of(date, time);
    }

    public static void writeOffsetDateTime(ObjectDataOutput out, OffsetDateTime value) throws IOException {
        writeLocalDateTime(out, value.toLocalDateTime());

        ZoneOffset offset = value.getOffset();
        int totalSeconds = offset.getTotalSeconds();
        out.writeInt(totalSeconds);
    }

    public static OffsetDateTime readOffsetDateTime(ObjectDataInput in) throws IOException {
        LocalDateTime dateTime = readLocalDateTime(in);

        int zoneTotalSeconds = in.readInt();
        ZoneOffset zoneOffset = ZoneOffset.ofTotalSeconds(zoneTotalSeconds);
        return OffsetDateTime.of(dateTime, zoneOffset);
    }
}
