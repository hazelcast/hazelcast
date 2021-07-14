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

package com.hazelcast.sql;

import com.hazelcast.config.Config;
import com.hazelcast.config.IndexType;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@SuppressWarnings("checkstyle:ParameterName")
public class SqlFieldNameResolutionTest extends SqlTestSupport {

    @BeforeClass
    public static void beforeClass() {
        final Config config = new Config();
        config.getJetConfig().setEnabled(true);
        initialize(1, config);
    }

    @Test
    public void when_publicFieldIsQueried_caseIsPreserved() {
        final IMap<Long, PublicFieldsPojo> testMap = instance().getMap("test");
        testMap.put(1L, new PublicFieldsPojo(1L, 2L, 3L));
        testMap.addIndex(IndexType.SORTED, "FIELD1", "field2", "fiELD3");

        final Map<String, Object> result = executeQuery("SELECT FIELD1, field2, fiELD3 FROM test");

        assertEquals(1L, result.get("FIELD1"));
        assertEquals(2L, result.get("field2"));
        assertEquals(3L, result.get("fiELD3"));
    }

    @Test
    public void when_privateFieldIsQueried_caseIsPreserved() {
        final IMap<Long, PrivateFieldsPojo> testMap = instance().getMap("test");
        testMap.put(1L, new PrivateFieldsPojo(1L, 2L, 3L, 4L, 5L));
        testMap.addIndex(IndexType.SORTED, "column1", "coluMn2", "coLuMn3", "COLUMN4", "cOLuMN5");

        final Map<String, Object> result = executeQuery("SELECT column1, coluMn2, coLuMn3, COLUMN4, cOLuMN5 "
                + "FROM test");

        assertEquals(1L, result.get("column1"));
        assertEquals(2L, result.get("coluMn2"));
        assertEquals(3L, result.get("coLuMn3"));
        assertEquals(4L, result.get("COLUMN4"));
        assertEquals(5L, result.get("cOLuMN5"));
    }

    @Test
    public void when_publicFieldIsInserted_newEntryIsPresent() {
        final IMap<Long, PublicFieldsPojo> testMap = instance().getMap("test");
        testMap.put(1L, new PublicFieldsPojo(1L, 2L, 3L));
        testMap.addIndex(IndexType.SORTED, "FIELD1", "field2", "fiELD3");

        instance().getSql().execute("INSERT INTO test (__key, FIELD1, field2, fiELD3) VALUES (2, 4, 5, 6)");

        final Map<String, Object> result = executeQuery("SELECT FIELD1, field2, fiELD3 FROM test "
                + "WHERE FIELD1 > 3 AND field2 > 4 AND fiELD3 > 5");
        assertEquals(4L, result.get("FIELD1"));
        assertEquals(5L, result.get("field2"));
        assertEquals(6L, result.get("fiELD3"));
    }

    @Test
    public void when_privateFieldIsInserted_newEntryIsPresent() {
        final IMap<Long, PrivateFieldsPojo> testMap = instance().getMap("test");
        testMap.put(1L, new PrivateFieldsPojo(1L, 2L, 3L, 4L, 5L));
        testMap.addIndex(IndexType.SORTED, "column1", "coluMn2", "coLuMn3", "COLUMN4", "cOLuMN5");

        instance().getSql().execute("INSERT INTO test (__key,column1, coluMn2, coLuMn3, COLUMN4, cOLuMN5) "
                + "VALUES (2, 6, 7, 8, 9, 10)");

        final Map<String, Object> result = executeQuery("SELECT column1, coluMn2, coLuMn3, COLUMN4, cOLuMN5 "
                + "FROM test WHERE column1 > 5 AND coluMn2 > 6 AND coLuMn3 > 7 AND COLUMN4 > 8 AND cOLuMN5 > 9");
        assertEquals(6L, result.get("column1"));
        assertEquals(7L, result.get("coluMn2"));
        assertEquals(8L, result.get("coLuMn3"));
        assertEquals(9L, result.get("COLUMN4"));
        assertEquals(10L, result.get("cOLuMN5"));
    }

    @Test
    public void when_privateFieldWithComplexCaseQueried_caseIsPreserved() {
        final IMap<Long, PrivateFieldsMixedCasePojo> testMap = instance().getMap("test");
        testMap.put(1L, new PrivateFieldsMixedCasePojo(1L));
        testMap.addIndex(IndexType.SORTED, "fLongFIELDWithComPlexCAse");

        final Map<String, Object> result = executeQuery("SELECT fLongFIELDWithComPlexCAse "
                + "FROM test WHERE fLongFIELDWithComPlexCAse > 0");

        assertEquals(1L, result.get("fLongFIELDWithComPlexCAse"));
    }


    private Map<String, Object> executeQuery(String sql) {
        final Map<String, Object> result = new HashMap<>();
        for (final SqlRow row : instance().getSql().execute(sql)) {
            final SqlRowMetadata metadata = row.getMetadata();
            for (int i = 0; i < metadata.getColumnCount(); i++) {
                result.put(metadata.getColumn(i).getName(), row.getObject(i));
            }
        }
        return result;
    }

    public static class PrivateFieldsPojo implements Serializable {
        private Long column1;
        private Long ColuMn2;
        private Long coLuMn3;
        private Long COLUMN4;
        public Long cOLuMN5;

        public PrivateFieldsPojo() {
        }

        public PrivateFieldsPojo(final Long column1, final Long coluMn2, final Long coLuMn3, final Long COLUMN4, final Long cOLuMN5) {
            this.column1 = column1;
            this.ColuMn2 = coluMn2;
            this.coLuMn3 = coLuMn3;
            this.COLUMN4 = COLUMN4;
            this.cOLuMN5 = cOLuMN5;
        }

        public Long getColumn1() {
            return column1;
        }

        public void setColumn1(final Long column1) {
            this.column1 = column1;
        }

        public Long getColuMn2() {
            return ColuMn2;
        }

        public void setColuMn2(final Long coluMn2) {
            ColuMn2 = coluMn2;
        }

        public Long getCoLuMn3() {
            return coLuMn3;
        }

        public void setCoLuMn3(final Long coLuMn3) {
            this.coLuMn3 = coLuMn3;
        }

        public Long getCOLUMN4() {
            return COLUMN4;
        }

        public void setCOLUMN4(final Long COLUMN4) {
            this.COLUMN4 = COLUMN4;
        }
    }

    public static class PublicFieldsPojo implements Serializable {
        public long FIELD1;
        public long field2;
        public long fiELD3;

        public PublicFieldsPojo() {
        }

        public PublicFieldsPojo(final long FIELD1, final long field2, final long fiELD3) {
            this.FIELD1 = FIELD1;
            this.field2 = field2;
            this.fiELD3 = fiELD3;
        }
    }

    public static class PrivateFieldsMixedCasePojo implements Serializable {
        private Long fLongFIELDWithComPlexCAse;

        public PrivateFieldsMixedCasePojo() {
        }

        public PrivateFieldsMixedCasePojo(final Long fLongFIELDWithComPlexCAse) {
            this.fLongFIELDWithComPlexCAse = fLongFIELDWithComPlexCAse;
        }

        public Long getFLongFIELDWithComPlexCAse() {
            return fLongFIELDWithComPlexCAse;
        }

        public void setFLongFIELDWithComPlexCAse(final Long fLongFIELDWithComPlexCAse) {
            this.fLongFIELDWithComPlexCAse = fLongFIELDWithComPlexCAse;
        }
    }
}
