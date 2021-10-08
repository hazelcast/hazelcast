/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql_slow;

import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.concurrent.ThreadLocalRandom;

import static com.hazelcast.jet.TestContextSupport.adaptSupplier;
import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("rawtypes")
@RunWith(HazelcastParallelClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class FuzzTestingTestCorpse extends SimpleTestInClusterSupport {

    private IMap<Long, FieldsCollection1> map1 = instance().getMap("map1");
    private IMap<Long, FieldsCollection2> map2 = instance().getMap("map2");
    private IMap<Long, FieldsCollection3> map3 = instance().getMap("map3");

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
    }

    @Ignore
    @Test
    public void noOOMTest() {
        map1 = instance().getMap("map1");
        map2 = instance().getMap("map2");
        map3 = instance().getMap("map3");
        for (int i = 0; i < 1000; ++i) {
            map1.put(getLong(), new FieldsCollection1(randomName(), getInt()));
            map2.put(getLong(), new FieldsCollection2(getBool(), getInt(), randomName()));
            map3.put(getLong(), new FieldsCollection3(getBool(), randomName(), getInt()));
        }

        SqlTestSupport.createMapping("map1", Long.class, FieldsCollection1.class);
        SqlTestSupport.createMapping("map2", Long.class, FieldsCollection2.class);
        SqlTestSupport.createMapping("map3", Long.class, FieldsCollection3.class);

        String sql = "SELECT map1.__key AS map1__key, " +
                "map1.c0 AS map1c0, " +
                "map1.c1 AS map1c1, " +
                "map2.__key AS map2__key, " +
                "map2.c0 AS map2c0, " +
                "map2.c1 AS map2c1, " +
                "map3.__key AS map3__key, " +
                "map3.c0 AS map3c0, " +
                "map3.c1 AS map3c1 " +
                "FROM map1, map2, map3 WHERE (NOT (((map2.__key) BETWEEN SYMMETRIC (map3.__key) AND (map1.__key)) BETWEEN (NOT (map2.c0)) AND (NOT (map3.c0)))) IS NULL " +
                "GROUP BY " +
                "map1.__key, map1.c0, map1.c1, " +
                "map2.__key, map2.c0, map2.c1, " +
                "map3.__key, map3.c0, map3.c1, map3.c2 " +
                "LIMIT 32767 OFFSET 0";
        assertThat(instance().getSql().execute(sql)).isEmpty();
        System.out.println("End");
    }

    private long getLong() {
        return ThreadLocalRandom.current().nextLong();
    }

    private int getInt() {
        return ThreadLocalRandom.current().nextInt();
    }

    private boolean getBool() {
        return ThreadLocalRandom.current().nextInt() % 2 == 0;
    }


    public static abstract class FieldsCollection implements Serializable {
    }

    public static class FieldsCollection1 extends FieldsCollection {

        public String c0;
        public int c1;

        @SuppressWarnings("unused")
        public FieldsCollection1() {
        }

        public FieldsCollection1(String c0, int c1) {
            this.c0 = c0;
            this.c1 = c1;
        }
    }

    public static class FieldsCollection2 extends FieldsCollection {

        public boolean c0;
        public int c1;
        public String c2;

        @SuppressWarnings("unused")
        public FieldsCollection2() {
        }

        public FieldsCollection2(boolean с0, int c1, String c2) {
            this.c0 = c0;
            this.c1 = c1;
            this.c2 = c2;
        }
    }

    public static class FieldsCollection3 extends FieldsCollection {

        public boolean c0;
        public String c1;
        public long c2;

        @SuppressWarnings("unused")
        public FieldsCollection3() {
        }

        public FieldsCollection3(boolean с0, String c1, long c2) {
            this.c0 = c0;
            this.c1 = c1;
            this.c2 = c2;
        }
    }
}

/*
 * SELECT
 * t0_tvuw59ZRvd.__key AS t0_tvuw59ZRvd__key,
 * t0_tvuw59ZRvd.c0 AS t0_tvuw59ZRvdc0,
 * t0_tvuw59ZRvd.c1 AS t0_tvuw59ZRvdc1,
 * t0_tvuw59ZRvd.c2 AS t0_tvuw59ZRvdc2,
 * t8_x6wtSXUKdR.__key AS t8_x6wtSXUKdR__key,
 * t8_x6wtSXUKdR.c0 AS t8_x6wtSXUKdRc0,
 * t10_LOMfzTUNyS.__key AS t10_LOMfzTUNyS__key,
 * t10_LOMfzTUNyS.c0 AS t10_LOMfzTUNySc0,
 * t10_LOMfzTUNyS.c1 AS t10_LOMfzTUNySc1
 * FROM t0_tvuw59ZRvd, t8_x6wtSXUKdR, t10_LOMfzTUNyS WHERE (NOT (((t8_x6wtSXUKdR.__key) BETWEEN SYMMETRIC (t10_LOMfzTUNyS.__key) AND (t8_x6wtSXUKdR.c0)) BETWEEN (NOT (t0_tvuw59ZRvd.c2)) AND (NOT (t0_tvuw59ZRvd.c2)))) IS NULL
 * GROUP BY t0_tvuw59ZRvd.__key, t0_tvuw59ZRvd.c0, t0_tvuw59ZRvd.c1, t0_tvuw59ZRvd.c2, t8_x6wtSXUKdR.__key, t8_x6wtSXUKdR.c0, t10_LOMfzTUNyS.__key, t10_LOMfzTUNyS.c0, t10_LOMfzTUNyS.c1
 * LIMIT 32767 OFFSET 0
 */