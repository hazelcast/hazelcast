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

package com.hazelcast.nio.serialization.compatibility;

import com.hazelcast.aggregation.Aggregators;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.projection.Projections;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.SampleTestObjects;

import java.io.Externalizable;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.CharBuffer;
import java.time.LocalTime;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.SynchronousQueue;

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
    static String aSmallString = "😊 Hello Приве́т नमस्ते שָׁלוֹם";

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

    static AbstractMap.SimpleEntry aSimpleMapEntry = new AbstractMap.SimpleEntry(aSmallString, anInnerPortable);
    static AbstractMap.SimpleImmutableEntry aSimpleImmutableMapEntry = new AbstractMap.SimpleImmutableEntry(aSmallString,
            anInnerPortable);

    static AnIdentifiedDataSerializable anIdentifiedDataSerializable = new AnIdentifiedDataSerializable(
            aBoolean, aByte, aChar, aDouble, aShort, aFloat, anInt, aLong, aSmallString,
            booleans, bytes, chars, doubles, shorts, floats, ints, longs, strings,
            anInnerPortable, null,
            aCustomStreamSerializable,
            aCustomByteArraySerializable, aData);
    static APortable aPortable = new APortable(
            aBoolean, aByte, aChar, aDouble, aShort, aFloat, anInt, aLong, aSmallString, anInnerPortable,
            booleans, bytes, chars, doubles, shorts, floats, ints, longs, strings, portables,
            anIdentifiedDataSerializable,
            aCustomStreamSerializable,
            aCustomByteArraySerializable, aData);

    static Date aDate;

    static LocalDate aLocalDate;
    static LocalTime aLocalTime;
    static LocalDateTime aLocalDateTime;
    static OffsetDateTime aOffsetDateTime;

    static {
        Calendar calendar = Calendar.getInstance();
        calendar.set(1990, Calendar.FEBRUARY, 1, 0, 0, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.ZONE_OFFSET, 0);
        aDate = calendar.getTime();
        aLocalDate = LocalDate.of(2021, 6, 28);
        aLocalTime = LocalTime.of(11, 22, 41, 123456789);
        aLocalDateTime = LocalDateTime.of(aLocalDate, aLocalTime);
        aOffsetDateTime = OffsetDateTime.of(aLocalDateTime, ZoneOffset.ofHours(18));
    }

    static BigInteger aBigInteger = new BigInteger("1314432323232411");
    static BigDecimal aBigDecimal = new BigDecimal(31231);
    static Class aClass = BigDecimal.class;
    static Optional<String> aFullOptional = Optional.of("SERIALIZEDSTRING");
    static Optional<String> anEmptyOptional = Optional.empty();
    static Enum anEnum = EntryEventType.ADDED;

    static Serializable serializable = new AJavaSerialiazable(anInt, aFloat);
    static Externalizable externalizable = new AJavaExternalizable(anInt, aFloat);

    static Comparable<SampleTestObjects.ValueType> aComparable = new SampleTestObjects.ValueType(aSmallString);

    static ArrayList nonNullList = new ArrayList(asList(
            aBoolean, aByte, aChar, aDouble, aShort, aFloat, anInt, aLong, aSmallString, anInnerPortable,
            booleans, bytes, chars, doubles, shorts, floats, ints, longs, strings,
            aCustomStreamSerializable, aCustomByteArraySerializable,
            anIdentifiedDataSerializable, aPortable,
            aDate, aLocalDate, aLocalTime, aLocalDateTime, aOffsetDateTime, aBigInteger, aBigDecimal, aClass,
            anEmptyOptional, aFullOptional, anEnum, aSimpleMapEntry, aSimpleImmutableMapEntry,
            serializable, externalizable));

    static ArrayList arrayList = new ArrayList(asList(aNullObject, nonNullList));

    static HashMap hashMap = new HashMap();

    static {
        nonNullList.forEach(e -> {
            if (e != null) {
                if (e instanceof String[] || e instanceof long[] || e instanceof int[] || e instanceof float[]
                        || e instanceof short[] || e instanceof double[] || e instanceof char[] || e instanceof byte[]
                        || e instanceof boolean[]) {
                    // skip these arrays since their equals methods don't work as expected inside the map equals method
                } else {
                    hashMap.put(e.getClass(), e);
                }
            }
        });
    }

    static LinkedList linkedList = new LinkedList(arrayList);
    static CopyOnWriteArrayList copyOnWriteArrayList = new CopyOnWriteArrayList(arrayList);

    static ConcurrentSkipListMap concurrentSkipListMap = new ConcurrentSkipListMap();
    static ConcurrentHashMap concurrentHashMap = new ConcurrentHashMap(hashMap);
    static LinkedHashMap linkedHashMap = new LinkedHashMap(hashMap);
    static TreeMap treeMap = new TreeMap();

    static HashSet hashSet = new HashSet(arrayList);
    static TreeSet treeSet = new TreeSet();
    static LinkedHashSet linkedHashSet = new LinkedHashSet(arrayList);
    static CopyOnWriteArraySet copyOnWriteArraySet = new CopyOnWriteArraySet(arrayList);
    static ConcurrentSkipListSet concurrentSkipListSet = new ConcurrentSkipListSet();
    static ArrayDeque arrayDeque = new ArrayDeque(nonNullList);
    static LinkedBlockingQueue linkedBlockingQueue = new LinkedBlockingQueue(nonNullList);
    static ArrayBlockingQueue arrayBlockingQueue = new ArrayBlockingQueue(5);
    static PriorityBlockingQueue priorityBlockingQueue = new PriorityBlockingQueue();
    static PriorityQueue priorityQueue = new PriorityQueue();
    static {
        arrayBlockingQueue.offer(aPortable);
        priorityBlockingQueue.offer(anInt);
        priorityQueue.offer(aSmallString);
    }
    static DelayQueue delayQueue = new DelayQueue();
    static SynchronousQueue synchronousQueue = new SynchronousQueue();
    static LinkedTransferQueue linkedTransferQueue = new LinkedTransferQueue(nonNullList);

    static Object[] allTestObjects = {
            aNullObject, aBoolean, aByte, aChar, aDouble, aShort, aFloat, anInt, aLong, aString, aUUID, anInnerPortable,
            aSimpleMapEntry, aSimpleImmutableMapEntry, booleans, bytes, chars, doubles, shorts, floats, ints, longs, strings,
            aCustomStreamSerializable, aCustomByteArraySerializable,
            anIdentifiedDataSerializable, aPortable,
            aDate, aLocalDate, aLocalTime, aLocalDateTime, aOffsetDateTime, aBigInteger, aBigDecimal, aClass,
            aFullOptional, anEnum, serializable, externalizable,
            arrayList, linkedList, copyOnWriteArrayList, concurrentSkipListMap, concurrentHashMap, linkedHashMap, treeMap,
            hashSet, treeSet, linkedHashSet, copyOnWriteArraySet, concurrentSkipListSet, arrayDeque, linkedBlockingQueue,
            arrayBlockingQueue, priorityQueue, priorityBlockingQueue, delayQueue, synchronousQueue, linkedTransferQueue,

            // predicates
            Predicates.alwaysTrue(),
            Predicates.alwaysFalse(),
            Predicates.sql(anSqlString),
            Predicates.equal(aSmallString, aComparable),
            Predicates.notEqual(aSmallString, aComparable),
            Predicates.greaterThan(aSmallString, aComparable),
            Predicates.between(aSmallString, aComparable, aComparable),
            Predicates.like(aSmallString, aSmallString),
            Predicates.ilike(aSmallString, aSmallString),
            Predicates.in(aSmallString, aComparable, aComparable),
            Predicates.regex(aSmallString, aSmallString),
            Predicates.partitionPredicate(aComparable, Predicates.greaterThan(aSmallString, aComparable)),
            Predicates.and(Predicates.sql(anSqlString),
                    Predicates.equal(aSmallString, aComparable),
                    Predicates.notEqual(aSmallString, aComparable),
                    Predicates.greaterThan(aSmallString, aComparable),
                    Predicates.greaterEqual(aSmallString, aComparable)),
            Predicates.or(Predicates.sql(anSqlString),
                    Predicates.equal(aSmallString, aComparable),
                    Predicates.notEqual(aSmallString, aComparable),
                    Predicates.greaterThan(aSmallString, aComparable),
                    Predicates.greaterEqual(aSmallString, aComparable)),
            Predicates.instanceOf(aCustomStreamSerializable.getClass()),

            // Aggregators
            Aggregators.distinct(aSmallString),
            Aggregators.integerMax(aSmallString),
            Aggregators.maxBy(aSmallString),
            Aggregators.comparableMin(aSmallString),
            Aggregators.minBy(aSmallString),
            Aggregators.count(aSmallString),
            Aggregators.numberAvg(aSmallString),
            Aggregators.integerAvg(aSmallString),
            Aggregators.longAvg(aSmallString),
            Aggregators.doubleAvg(aSmallString),
            Aggregators.bigIntegerAvg(aSmallString),
            Aggregators.bigDecimalAvg(aSmallString),
            Aggregators.integerSum(aSmallString),
            Aggregators.longSum(aSmallString),
            Aggregators.doubleSum(aSmallString),
            Aggregators.fixedPointSum(aSmallString),
            Aggregators.floatingPointSum(aSmallString),
            Aggregators.bigDecimalSum(aSmallString),

            // projections
            Projections.singleAttribute(aSmallString),
            Projections.multiAttribute(aSmallString, aSmallString, anSqlString),
            Projections.identity()
    };
}
