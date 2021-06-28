package com.hazelcast.internal.nio;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.time.*;


@SuppressWarnings({ "WeakerAccess", "checkstyle:methodcount", "checkstyle:magicnumber", "checkstyle:classfanoutcomplexity",
        "checkstyle:ClassDataAbstractionCoupling" })
public final class PortableUtil {
    // for year field, short used here for backward compatibility

    public static void writeLocalTime(ObjectDataOutput out, LocalTime value) throws IOException {
        int hour = value.getHour();
        int minute = value.getMinute();
        int second = value.getSecond();
        int nano = value.getNano();
        out.writeByte(hour);
        out.writeByte(minute);
        out.writeByte(second);
        out.writeInt(nano);
    }

    public static LocalTime readLocalTime(ObjectDataInput in) throws IOException {
        int hour = in.readByte();
        int minute = in.readByte();
        int second = in.readByte();
        int nano = in.readInt();
        return LocalTime.of(hour, minute, second, nano);
    }

    public static void writeLocalDate(ObjectDataOutput out, LocalDate value) throws IOException {
        int year = value.getYear();
        int monthValue = value.getMonthValue();
        int dayOfMonth = value.getDayOfMonth();
        out.writeShort(year);
        out.writeByte(monthValue);
        out.writeByte(dayOfMonth);
    }

    public static LocalDate readLocalDate(ObjectDataInput in) throws IOException {
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
        int year = in.readShort();
        int month = in.readByte();
        int dayOfMonth = in.readByte();

        int hour = in.readByte();
        int minute = in.readByte();
        int second = in.readByte();
        int nano = in.readInt();

        return LocalDateTime.of(year, month, dayOfMonth, hour, minute, second, nano);
    }

    public static void writeOffsetDateTime(ObjectDataOutput out, OffsetDateTime value) throws IOException {
        writeLocalDate(out, value.toLocalDate());
        writeLocalTime(out, value.toLocalTime());

        ZoneOffset offset = value.getOffset();
        int totalSeconds = offset.getTotalSeconds();
        out.writeInt(totalSeconds);
    }

    public static OffsetDateTime readOffsetDateTime(ObjectDataInput in) throws IOException {
        int year = in.readShort();
        int month = in.readByte();
        int dayOfMonth = in.readByte();

        int hour = in.readByte();
        int minute = in.readByte();
        int second = in.readByte();
        int nano = in.readInt();

        int zoneTotalSeconds = in.readInt();
        ZoneOffset zoneOffset = ZoneOffset.ofTotalSeconds(zoneTotalSeconds);
        return OffsetDateTime.of(year, month, dayOfMonth, hour, minute, second, nano, zoneOffset);
    }
}
