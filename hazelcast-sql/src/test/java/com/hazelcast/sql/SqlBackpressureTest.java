/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.support.SqlTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SqlBackpressureTest extends SqlTestSupport {
    /** Base query. */
    private static final String SQL_BASE =
        "SELECT l01, l02, l03, l04, l05, l06, l07, l08, l09, l10, l11, l12, l13, l14, l15, l16 FROM map ";

    // 128 - row width, 128 * 8 = 1Kb, 1Kb * 1024 = 1Mb, 1Mb * 5 - senders will need at least 2 control flow messages.
    private static final int ROW_COUNT = 7 * 8 * 1024;

    private static TestHazelcastInstanceFactory factory;
    private static HazelcastInstance member;

    private static long key = 0;

    @BeforeClass
    public static void beforeClass() {
        factory = new TestHazelcastInstanceFactory(2);

        member = factory.newHazelcastInstance();
        factory.newHazelcastInstance();
    }

    @AfterClass
    public static void afterClass() {
        if (factory != null) {
            factory.shutdownAll();
        }
    }

    private static void load() {
        IMap<Long, LongValue> map = member.getMap("map");

        Map<Long, LongValue> localMap = new HashMap<>();

        for (long i = 0; i < ROW_COUNT; i++) {
            localMap.put(key++, new LongValue(i));
        }

        map.putAll(localMap);
    }

    private static void reload() {
        member.getMap("map").clear();

        load();
    }

    @Test
    public void testBackpressure() {
        reload();

        List<SqlRow> rows = getQueryRows(
            member,
            SQL_BASE
        );

        assertEquals(ROW_COUNT, rows.size());
    }

    @Test
    public void testBackpressureLimit() {
        reload();

        List<SqlRow> rows = getQueryRows(
            member,
            SQL_BASE + "LIMIT 200 OFFSET 100"
        );

        assertEquals(200, rows.size());
    }

    @Test
    public void testBackpressureWithSorting() {
        reload();

        List<SqlRow> rows = getQueryRows(
            member,
            SQL_BASE + "ORDER BY l01"
        );

        assertEquals(ROW_COUNT, rows.size());
    }

    @Test
    public void testBackpressureWithSortingLimit() {
        reload();

        List<SqlRow> rows = getQueryRows(
            member,
            SQL_BASE + "ORDER BY l01 LIMIT 200 OFFSET 100"
        );

        assertEquals(200, rows.size());
    }

    @SuppressWarnings("unused")
    private static class LongValue implements Serializable {
        public long l01;
        public long l02;
        public long l03;
        public long l04;
        public long l05;
        public long l06;
        public long l07;
        public long l08;
        public long l09;
        public long l10;
        public long l11;
        public long l12;
        public long l13;
        public long l14;
        public long l15;
        public long l16;

        private LongValue(long val) {
            this.l01 = val;
            this.l02 = val;
            this.l03 = val;
            this.l04 = val;
            this.l05 = val;
            this.l06 = val;
            this.l07 = val;
            this.l08 = val;
            this.l09 = val;
            this.l10 = val;
            this.l11 = val;
            this.l12 = val;
            this.l13 = val;
            this.l14 = val;
            this.l15 = val;
            this.l16 = val;
        }

        public long getL01() {
            return l01;
        }

        public long getL02() {
            return l02;
        }

        public long getL03() {
            return l03;
        }

        public long getL04() {
            return l04;
        }

        public long getL05() {
            return l05;
        }

        public long getL06() {
            return l06;
        }

        public long getL07() {
            return l07;
        }

        public long getL08() {
            return l08;
        }

        public long getL09() {
            return l09;
        }

        public long getL10() {
            return l10;
        }

        public long getL11() {
            return l11;
        }

        public long getL12() {
            return l12;
        }

        public long getL13() {
            return l13;
        }

        public long getL14() {
            return l14;
        }

        public long getL15() {
            return l15;
        }

        public long getL16() {
            return l16;
        }
    }
}
