package com.hazelcast.nio.serialization.compatibility;

import com.hazelcast.aggregation.Aggregators;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.projection.Projections;
import com.hazelcast.query.Predicates;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.CharBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.UUID;

import static java.util.Arrays.asList;

class ReferenceObjects {

    /**
     * PORTABLE IDS
     **/
    static int PORTABLE_FACTORY_ID = 1;
    static int PORTABLE_CLASS_ID = 1;
    static int INNER_PORTABLE_CLASS_ID = 2;

    /**
     * IDENTIFIED DATA SERIALIZABLE IDS
     **/
    static int IDENTIFIED_DATA_SERIALIZABLE_FACTORY_ID = 1;
    static int DATA_SERIALIZABLE_CLASS_ID = 1;

    /**
     * CUSTOM SERIALIZER IDS
     */
    static int CUSTOM_STREAM_SERIALIZABLE_ID = 1;
    static int CUSTOM_BYTE_ARRAY_SERIALIZABLE_ID = 2;

    /**
     * OBJECTS
     */
    static Object aNullObject = null;
    static boolean aBoolean = true;
    static byte aByte = 113;
    static char aChar = 'x';
    static double aDouble = -897543.3678909d;
    static short aShort = -500;
    static float aFloat = 900.5678f;
    static int anInt = 56789;
    static long aLong = -50992225L;
    static String aString;
    static String anSqlString = "this > 5 AND this < 100";
    static UUID aUUID = new UUID(aLong, anInt);

    static {
        CharBuffer cb = CharBuffer.allocate(Character.MAX_VALUE);
        for (char c = 0; c < Character.MAX_VALUE; c++) {
            if (!(c >= Character.MIN_SURROGATE && c < (Character.MAX_SURROGATE + 1))) {
                cb.append(c);
            }
        }
        aString = new String(cb.array());
    }

    static boolean[] booleans = {true, false, true};

    static byte[] bytes = {112, 4, -1, 4, 112, -35, 43};
    static char[] chars = {'a', 'b', 'c'};
    static double[] doubles = {-897543.3678909d, 11.1d, 22.2d, 33.3d};
    static short[] shorts = {-500, 2, 3};
    static float[] floats = {900.5678f, 1.0f, 2.1f, 3.4f};
    static int[] ints = {56789, 2, 3};
    static long[] longs = {-50992225L, 1231232141L, 2L, 3L};
    static String[] strings = {
            "Pijamalı hasta, yağız şoföre çabucak güvendi.",
            "イロハニホヘト チリヌルヲ ワカヨタレソ ツネナラム",
            "The quick brown fox jumps over the lazy dog",
    };

    static Data aData = new HeapData("111313123131313131".getBytes());

    static AnInnerPortable anInnerPortable = new AnInnerPortable(anInt, aFloat);
    static CustomStreamSerializable aCustomStreamSerializable = new CustomStreamSerializable(anInt, aFloat);
    static CustomByteArraySerializable aCustomByteArraySerializable = new CustomByteArraySerializable(anInt, aFloat);
    static Portable[] portables = {anInnerPortable, anInnerPortable, anInnerPortable};

    static AnIdentifiedDataSerializable anIdentifiedDataSerializable = new AnIdentifiedDataSerializable(
            aBoolean, aByte, aChar, aDouble, aShort, aFloat, anInt, aLong, anSqlString,
            booleans, bytes, chars, doubles, shorts, floats, ints, longs, strings,
            anInnerPortable, null,
            aCustomStreamSerializable,
            aCustomByteArraySerializable, aData);
    static LocalDate aLocalDate;
    static LocalTime aLocalTime;
    static LocalDateTime aLocalDateTime;
    static OffsetDateTime anOffsetDateTime;

    static BigDecimal aBigDecimal = new BigDecimal("31231.12331");
    static BigDecimal[] bigDecimals = {aBigDecimal, aBigDecimal, aBigDecimal};

    static {
        Calendar calendar = Calendar.getInstance();
        calendar.set(1990, Calendar.FEBRUARY, 1, 0, 0, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.ZONE_OFFSET, 0);
        aLocalDate = LocalDate.of(2021, 6, 28);
        aLocalTime = LocalTime.of(11, 22, 41, 123456000);
        aLocalDateTime = LocalDateTime.of(aLocalDate, aLocalTime);
        anOffsetDateTime = OffsetDateTime.of(aLocalDateTime, ZoneOffset.ofHours(18));
    }
    static LocalDate[] localDates = {aLocalDate, aLocalDate, aLocalDate};
    static LocalTime[] localTimes = {aLocalTime, aLocalTime, aLocalTime};
    static LocalDateTime[] localDateTimes = {aLocalDateTime, aLocalDateTime, aLocalDateTime};
    static OffsetDateTime[] offsetDateTimes = {anOffsetDateTime, anOffsetDateTime, anOffsetDateTime};

    static APortable aPortable = new APortable(
            aBoolean, aByte, aChar, aDouble, aShort, aFloat, anInt, aLong, anSqlString, aBigDecimal,
            aLocalDate, aLocalTime, aLocalDateTime, anOffsetDateTime, anInnerPortable,
            booleans, bytes, chars, doubles, shorts, floats, ints, longs, strings, bigDecimals,
            localDates, localTimes, localDateTimes, offsetDateTimes, portables,
            anIdentifiedDataSerializable,
            aCustomStreamSerializable,
            aCustomByteArraySerializable, aData);

    static BigInteger aBigInteger = new BigInteger("1314432323232411");
    static Class aClass = BigDecimal.class;

    static ArrayList nonNullList = new ArrayList(asList(
            aBoolean, aDouble, anInt, anSqlString, anInnerPortable,
            bytes, aCustomStreamSerializable, aCustomByteArraySerializable,
            anIdentifiedDataSerializable, aPortable,
            aBigDecimal, aLocalDate, aLocalTime, anOffsetDateTime));

    static ArrayList arrayList = new ArrayList(asList(aNullObject, nonNullList));

    static LinkedList linkedList = new LinkedList(arrayList);

    static Object[] allTestObjects = {
            aNullObject, aBoolean, aByte, aChar, aDouble, aShort, aFloat, anInt, aLong, aString, aUUID, anInnerPortable,
            booleans, bytes, chars, doubles, shorts, floats, ints, longs, strings,
            aCustomStreamSerializable, aCustomByteArraySerializable,
            anIdentifiedDataSerializable, aPortable,
            aLocalDate, aLocalTime, aLocalDateTime, anOffsetDateTime, aBigInteger, aBigDecimal, aClass,
            arrayList, linkedList,

            // predicates
            Predicates.alwaysTrue(),
            Predicates.alwaysFalse(),
            Predicates.sql(anSqlString),
            Predicates.equal(anSqlString, anInt),
            Predicates.notEqual(anSqlString, anInt),
            Predicates.greaterThan(anSqlString, anInt),
            Predicates.between(anSqlString, anInt, anInt),
            Predicates.like(anSqlString, anSqlString),
            Predicates.ilike(anSqlString, anSqlString),
            Predicates.in(anSqlString, anInt, anInt),
            Predicates.regex(anSqlString, anSqlString),
            Predicates.and(Predicates.sql(anSqlString),
                    Predicates.equal(anSqlString, anInt),
                    Predicates.notEqual(anSqlString, anInt),
                    Predicates.greaterThan(anSqlString, anInt),
                    Predicates.greaterEqual(anSqlString, anInt)),
            Predicates.or(Predicates.sql(anSqlString),
                    Predicates.equal(anSqlString, anInt),
                    Predicates.notEqual(anSqlString, anInt),
                    Predicates.greaterThan(anSqlString, anInt),
                    Predicates.greaterEqual(anSqlString, anInt)),
            Predicates.instanceOf(aCustomStreamSerializable.getClass()),

            // Aggregators
            Aggregators.distinct(anSqlString),
            Aggregators.integerMax(anSqlString),
            Aggregators.maxBy(anSqlString),
            Aggregators.comparableMin(anSqlString),
            Aggregators.minBy(anSqlString),
            Aggregators.count(anSqlString),
            Aggregators.numberAvg(anSqlString),
            Aggregators.integerAvg(anSqlString),
            Aggregators.longAvg(anSqlString),
            Aggregators.doubleAvg(anSqlString),
            Aggregators.integerSum(anSqlString),
            Aggregators.longSum(anSqlString),
            Aggregators.doubleSum(anSqlString),
            Aggregators.fixedPointSum(anSqlString),
            Aggregators.floatingPointSum(anSqlString),
            Aggregators.bigDecimalSum(anSqlString),

            // projections
            Projections.singleAttribute(anSqlString),
            Projections.multiAttribute(anSqlString, anSqlString, anSqlString),
            Projections.identity()

    };
}