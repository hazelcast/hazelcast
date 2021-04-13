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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.map.model.Person;
import com.hazelcast.jet.sql.impl.connector.map.model.PersonId;
import com.hazelcast.jet.sql.impl.connector.test.TestBatchSqlConnector;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.QueryException;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SqlJoinTest extends SqlTestSupport {

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(2, null);
        sqlService = instance().getSql();
    }

    @Test
    public void test_innerJoin() {
        String leftName = randomName();
        TestBatchSqlConnector.create(sqlService, leftName, 3);

        String mapName = randomName();
        instance().getMap(mapName).put(1, "value-1");
        instance().getMap(mapName).put(2, "value-2");
        instance().getMap(mapName).put(3, "value-3");

        assertRowsAnyOrder(
                "SELECT l.v, m.this " +
                        "FROM " + leftName + " l " +
                        "INNER JOIN " + mapName + " m ON l.v = m.__key",
                asList(
                        new Row(1, "value-1"),
                        new Row(2, "value-2")
                )
        );
    }

    @Test
    public void test_innerJoinUsing() {
        String leftName = randomName();
        TestBatchSqlConnector.create(
                sqlService,
                leftName,
                singletonList("__key"),
                singletonList(INT),
                asList(new String[]{"0"}, new String[]{"1"}, new String[]{"2"})
        );

        String mapName = randomName();
        instance().getMap(mapName).put(1, "value-1");
        instance().getMap(mapName).put(2, "value-2");
        instance().getMap(mapName).put(3, "value-3");

        assertRowsAnyOrder(
                "SELECT l.__key, m.this " +
                        "FROM " + leftName + " l " +
                        "JOIN " + mapName + " m USING (__key)",
                asList(
                        new Row(1, "value-1"),
                        new Row(2, "value-2")
                )
        );
    }

    @Test
    public void test_innerJoinConditionInWhereClause() {
        String leftName = randomName();
        TestBatchSqlConnector.create(sqlService, leftName, 3);

        String mapName = randomName();
        instance().getMap(mapName).put(1, "value-1");
        instance().getMap(mapName).put(2, "value-2");
        instance().getMap(mapName).put(3, "value-3");

        assertRowsAnyOrder(
                "SELECT l.v, m.this " +
                        "FROM " + leftName + " l, " + mapName + " m " +
                        "WHERE l.v = m.__key",
                asList(
                        new Row(1, "value-1"),
                        new Row(2, "value-2")
                )
        );
    }

    @Test
    public void test_innerJoinAndConditionInWhereClause() {
        String leftName = randomName();
        TestBatchSqlConnector.create(sqlService, leftName, 3);

        String mapName = randomName();
        instance().getMap(mapName).put(1, "value-1");
        instance().getMap(mapName).put(2, "value-2");
        instance().getMap(mapName).put(3, "value-3");

        // TODO assert that it uses the join-primitive plan
        assertRowsAnyOrder(
                "SELECT l.v, m.this " +
                        "FROM " + leftName + " l " +
                        "INNER JOIN " + mapName + " m ON 1 = 1 " +
                        "WHERE l.v = m.__key",
                asList(
                        new Row(1, "value-1"),
                        new Row(2, "value-2")
                )
        );
    }

    @Test
    public void test_innerJoinWithoutCondition() {
        String leftName = randomName();
        TestBatchSqlConnector.create(sqlService, leftName, 2);

        String mapName = randomName();
        instance().getMap(mapName).put(1, "value-1");
        instance().getMap(mapName).put(2, "value-2");

        assertRowsAnyOrder(
                "SELECT l.v, m.this " +
                        "FROM " + leftName + " l, " + mapName + " m ",
                asList(
                        new Row(0, "value-1"),
                        new Row(0, "value-2"),
                        new Row(1, "value-1"),
                        new Row(1, "value-2")
                )
        );
    }

    @Test
    public void test_crossJoin() {
        String leftName = randomName();
        TestBatchSqlConnector.create(sqlService, leftName, 2);

        String mapName = randomName();
        instance().getMap(mapName).put(1, "value-1");
        instance().getMap(mapName).put(2, "value-2");

        assertRowsAnyOrder(
                "SELECT l.v, m.this " +
                        "FROM " + leftName + " l " +
                        "CROSS JOIN " + mapName + " m ",
                asList(
                        new Row(0, "value-1"),
                        new Row(0, "value-2"),
                        new Row(1, "value-1"),
                        new Row(1, "value-2")
                )
        );
    }

    @Test
    public void test_innerJoinNull() {
        String leftName = randomName();
        TestBatchSqlConnector.create(
                sqlService,
                leftName,
                singletonList("v"),
                singletonList(INT),
                asList(new String[]{"0"}, new String[]{null}, new String[]{"2"})
        );

        String mapName = randomName();
        instance().getMap(mapName).put(1, "value-1");
        instance().getMap(mapName).put(2, "value-2");
        instance().getMap(mapName).put(3, "value-3");

        assertRowsAnyOrder(
                "SELECT l.v, m.this " +
                        "FROM " + leftName + " l " +
                        "JOIN " + mapName + " m ON l.v = m.__key",
                singletonList(new Row(2, "value-2"))
        );
    }

    @Test
    public void test_innerJoinFilter() {
        String leftName = randomName();
        TestBatchSqlConnector.create(sqlService, leftName, 3);

        String mapName = randomName();
        instance().getMap(mapName).put(1, "value-1");
        instance().getMap(mapName).put(2, "value-2");
        instance().getMap(mapName).put(3, "value-3");

        assertRowsAnyOrder(
                "SELECT l.v, m.this " +
                        "FROM " + leftName + " l " +
                        "JOIN " + mapName + " m ON l.v = m.__key " +
                        "WHERE m.__key < 2",
                singletonList(new Row(1, "value-1"))
        );
    }

    @Test
    public void test_innerJoinProject() {
        String leftName = randomName();
        TestBatchSqlConnector.create(sqlService, leftName, 3);

        String mapName = randomName();
        instance().getMap(mapName).put(1, "value-1");
        instance().getMap(mapName).put(2, "value-2");
        instance().getMap(mapName).put(3, "value-3");

        assertRowsAnyOrder(
                "SELECT l.v, m.this || '-s' " +
                        "FROM " + leftName + " l " +
                        "JOIN " + mapName + " m ON l.v = m.__key ",
                asList(
                        new Row(1, "value-1-s"),
                        new Row(2, "value-2-s")
                )
        );
    }

    @Test
    public void test_innerJoinConditionProject() {
        String leftName = randomName();
        TestBatchSqlConnector.create(sqlService, leftName, 3);

        String mapName = randomName();
        instance().getMap(mapName).put(1, "value-1");
        instance().getMap(mapName).put(2, "value-2");
        instance().getMap(mapName).put(3, "value-3");

        assertRowsAnyOrder(
                "SELECT l.v, m.__key, m.this " +
                        "FROM " + leftName + " l " +
                        "JOIN " + mapName + " m ON l.v = 2 * m.__key",
                singletonList(new Row(2, 1, "value-1"))
        );
    }

    @Test
    public void test_innerJoinOnValue() {
        String leftName = randomName();
        TestBatchSqlConnector.create(sqlService, leftName, 3);

        String mapName = randomName();
        instance().getMap(mapName).put("value-1", 1);
        instance().getMap(mapName).put("value-2", 2);
        instance().getMap(mapName).put("value-3", 3);

        assertRowsAnyOrder(
                "SELECT l.v, m.__key " +
                        "FROM " + leftName + " l " +
                        "JOIN " + mapName + " m ON l.v = m.this",
                asList(
                        new Row(1, "value-1"),
                        new Row(2, "value-2")
                )
        );
    }

    @Test
    public void test_innerJoinNonEqui() {
        String leftName = randomName();
        TestBatchSqlConnector.create(sqlService, leftName, 4);

        String mapName = randomName();
        instance().getMap(mapName).put(1, "value-1");
        instance().getMap(mapName).put(2, "value-2");
        instance().getMap(mapName).put(3, "value-3");

        assertRowsAnyOrder(
                "SELECT l.v, m.__key, m.this " +
                        "FROM " + leftName + " l " +
                        "JOIN " + mapName + " m ON l.v > m.__key",
                asList(
                        new Row(2, 1, "value-1"),
                        new Row(3, 1, "value-1"),
                        new Row(3, 2, "value-2")
                )
        );
    }

    @Test
    public void test_joinEquiJoinAndDisjunction() {
        String leftName = randomName();
        TestBatchSqlConnector.create(sqlService, leftName, 4);

        String mapName = randomName();
        instance().getMap(mapName).put(1, "value-1");
        instance().getMap(mapName).put(2, "value-2");
        instance().getMap(mapName).put(3, "value-3");

        // this currently uses the full-scan join
        assertRowsAnyOrder(
                "SELECT l.v, m.__key, m.this " +
                        "FROM " + leftName + " l " +
                        "JOIN " + mapName + " m ON l.v = m.__key OR l.v = m.__key",
                asList(
                        new Row(1, 1, "value-1"),
                        new Row(2, 2, "value-2"),
                        new Row(3, 3, "value-3")
                )
        );
    }

    @Test
    public void test_innerJoinEquiAndNonEqui() {
        String leftName = randomName();
        TestBatchSqlConnector.create(
                sqlService,
                leftName,
                asList("v1", "v2"),
                asList(INT, INT),
                asList(new String[]{"0", "0"}, new String[]{"1", "0"}, new String[]{"2", "2"})
        );

        String mapName = randomName();
        instance().getMap(mapName).put(1, "value-1");
        instance().getMap(mapName).put(2, "value-2");
        instance().getMap(mapName).put(3, "value-3");

        assertRowsAnyOrder(
                "SELECT l.v1, l.v2, m.__key, m.this " +
                        "FROM " + leftName + " l " +
                        "JOIN " + mapName + " m ON l.v1 = m.__key AND l.v2 != m.__key",
                singletonList(new Row(1, 0, 1, "value-1"))
        );
    }

    @Test
    public void test_innerJoinMulti() {
        String leftName = randomName();
        TestBatchSqlConnector.create(sqlService, leftName, 3);

        String mapName1 = randomName();
        instance().getMap(mapName1).put(1, "value-1.1");
        instance().getMap(mapName1).put(2, "value-1.2");
        instance().getMap(mapName1).put(3, "value-1.3");

        String mapName2 = randomName();
        instance().getMap(mapName2).put(1, "value-2.1");
        instance().getMap(mapName2).put(2, "value-2.2");
        instance().getMap(mapName2).put(3, "value-2.3");

        assertRowsAnyOrder(
                "SELECT l.v, m1.this, m2.this " +
                        "FROM " + leftName + " l " +
                        "JOIN " + mapName1 + " m1 ON l.v = m1.__key " +
                        "JOIN " + mapName2 + " m2 ON l.v + m1.__key > m2.__key",
                asList(
                        new Row(1, "value-1.1", "value-2.1"),
                        new Row(2, "value-1.2", "value-2.1"),
                        new Row(2, "value-1.2", "value-2.2"),
                        new Row(2, "value-1.2", "value-2.3")
                )
        );
    }

    @Test
    public void test_innerJoinPartOfTheCompositeKey() {
        String leftName = randomName();
        TestBatchSqlConnector.create(
                sqlService,
                leftName,
                singletonList("v"),
                singletonList(INT),
                asList(new String[]{"0"}, new String[]{null}, new String[]{"2"})
        );

        String mapName = randomName();
        instance().getMap(mapName).put(new Person(null, "value-1"), new PersonId());
        instance().getMap(mapName).put(new Person(2, "value-2"), new PersonId());
        instance().getMap(mapName).put(new Person(3, "value-3"), new PersonId());

        assertRowsEventuallyInAnyOrder(
                "SELECT l.v, m.name, m.id " +
                        "FROM " + leftName + " l " +
                        "JOIN " + mapName + " m ON l.v = m.id",
                singletonList(new Row(2, "value-2", 2))
        );
    }

    @Test
    public void test_innerJoinFullCompositeKeyConjunction() {
        String leftName = randomName();
        TestBatchSqlConnector.create(
                sqlService,
                leftName,
                asList("v1", "v2"),
                asList(INT, VARCHAR),
                asList(new String[]{"0", "value-0"}, new String[]{"1", null}, new String[]{"2", "value-2"})
        );

        String mapName = randomName();
        instance().getMap(mapName).put(new Person(1, null), new PersonId());
        instance().getMap(mapName).put(new Person(2, "value-2"), new PersonId());
        instance().getMap(mapName).put(new Person(3, "value-3"), new PersonId());

        assertRowsEventuallyInAnyOrder(
                "SELECT l.v1, l.v2, m.id, m.name " +
                        "FROM " + leftName + " l " +
                        "JOIN " + mapName + " m ON l.v1 = m.id AND l.v2 = m.name",
                singletonList(new Row(2, "value-2", 2, "value-2"))
        );
    }

    @Test
    public void test_innerJoinFullCompositeKeyDisjunction() {
        String leftName = randomName();
        TestBatchSqlConnector.create(
                sqlService,
                leftName,
                asList("v1", "v2"),
                asList(INT, VARCHAR),
                asList(new String[]{"0", "value-0"}, new String[]{"1", null}, new String[]{"2", "value-2"})
        );

        String mapName = randomName();
        instance().getMap(mapName).put(new Person(1, "value-1"), new PersonId());
        instance().getMap(mapName).put(new Person(2, "value-2"), new PersonId());
        instance().getMap(mapName).put(new Person(3, "value-3"), new PersonId());

        assertRowsEventuallyInAnyOrder(
                "SELECT l.v1, l.v2, m.id, m.name " +
                        "FROM " + leftName + " l " +
                        "JOIN " + mapName + " m ON l.v1 = m.id OR l.v2 = m.name",
                asList(
                        new Row(1, null, 1, "value-1"),
                        new Row(2, "value-2", 2, "value-2")
                )
        );
    }

    @Test
    public void test_innerJoinPartOfTheCompositeValue() {
        String leftName = randomName();
        TestBatchSqlConnector.create(
                sqlService,
                leftName,
                singletonList("v"),
                singletonList(VARCHAR),
                asList(new String[]{"value-0"}, new String[]{"value-1"}, new String[]{"value-2"})
        );

        String mapName = randomName();
        instance().getMap(mapName).put(new PersonId(1), new Person(0, "value-1"));
        instance().getMap(mapName).put(new PersonId(2), new Person(0, "value-2"));
        instance().getMap(mapName).put(new PersonId(3), new Person(0, "value-3"));

        assertRowsEventuallyInAnyOrder(
                "SELECT l.v, m.id " +
                        "FROM " + leftName + " l " +
                        "JOIN " + mapName + " m ON l.v = m.name",
                asList(
                        new Row("value-1", 1),
                        new Row("value-2", 2)
                )
        );
    }

    @Test
    public void test_innerJoinKeyAndValue() {
        String leftName = randomName();
        TestBatchSqlConnector.create(sqlService, leftName, 3);

        String mapName = randomName();
        instance().getMap(mapName).put(1, new Person(0, "value-1"));
        instance().getMap(mapName).put(2, new Person(2, "value-2"));
        instance().getMap(mapName).put(3, new Person(0, "value-3"));

        assertRowsEventuallyInAnyOrder(
                "SELECT l.v, m.id, m.name " +
                        "FROM " + leftName + " l " +
                        "JOIN " + mapName + " m ON l.v = m.__key AND l.v = m.id",
                singletonList(new Row(2, 2, "value-2"))
        );
    }

    @Test
    public void test_innerJoinWithAlwaysFalseCondition() {
        String leftName = randomName();
        TestBatchSqlConnector.create(sqlService, leftName, 4);

        String mapName = randomName();
        instance().getMap(mapName).put(1, "value-1");
        instance().getMap(mapName).put(2, "value-2");
        instance().getMap(mapName).put(3, "value-3");

        assertRowsAnyOrder(
                "SELECT l.v, m.__key, m.this " +
                        "FROM " + leftName + " l " +
                        "JOIN " + mapName + " m ON 1 = 2",
                emptyList()
        );
    }

    @Test
    public void test_innerJoinWithTypeConversion_smallerLeft() {
        String leftName = randomName();
        TestBatchSqlConnector.create(sqlService, leftName, 3);

        String mapName = randomName();
        instance().getMap(mapName).put((short) 1, "value-1");
        instance().getMap(mapName).put((short) 2, "value-2");
        instance().getMap(mapName).put((short) 3, "value-3");

        assertRowsAnyOrder(
                "SELECT l.v, m.this " +
                        "FROM " + leftName + " l " +
                        "INNER JOIN " + mapName + " m ON l.v = m.__key",
                asList(
                        new Row(1, "value-1"),
                        new Row(2, "value-2")
                )
        );
    }

    @Test
    public void test_innerJoinWithTypeConversion_smallerRight() {
        String leftName = randomName();
        TestBatchSqlConnector.create(sqlService, leftName, 3);

        String mapName = randomName();
        instance().getMap(mapName).put(1L, "value-1");
        instance().getMap(mapName).put(2L, "value-2");
        instance().getMap(mapName).put(3L, "value-3");

        assertRowsAnyOrder(
                "SELECT l.v, m.this " +
                        "FROM " + leftName + " l " +
                        "INNER JOIN " + mapName + " m ON l.v = m.__key",
                asList(
                        new Row(1, "value-1"),
                        new Row(2, "value-2")
                )
        );
    }

    @Test
    public void test_joinSubquery() {
        String leftName = randomName();
        TestBatchSqlConnector.create(sqlService, leftName, 1);

        String mapName = randomName();
        instance().getMap(mapName).put(1, "value-1");

        assertThatThrownBy(() ->
                sqlService.execute(
                        "SELECT 1 " +
                                "FROM " + leftName + " AS l " +
                                "JOIN (SELECT * FROM " + mapName + ") AS m ON l.v = m.__key"
                ))
                .hasCauseInstanceOf(QueryException.class)
                .hasMessageContaining("Sub-query not supported on the right side of a join");
    }

    @Test
    public void test_joinValues() {
        String leftName = randomName();
        TestBatchSqlConnector.create(sqlService, leftName, 1);

        assertThatThrownBy(() ->
                sqlService.execute(
                        "SELECT * FROM " + leftName + " l JOIN (VALUES (1)) AS r (__key) ON l.v = r.__key"
                ))
                .hasCauseInstanceOf(QueryException.class)
                .hasMessageContaining("VALUES clause not supported on the right side of a join");
    }

    @Test
    public void test_leftJoin() {
        String leftName = randomName();
        TestBatchSqlConnector.create(
                sqlService,
                leftName,
                singletonList("v"),
                singletonList(INT),
                asList(new String[]{"0"}, new String[]{null}, new String[]{"2"})
        );

        String mapName = randomName();
        instance().getMap(mapName).put(1, "value-1");
        instance().getMap(mapName).put(2, "value-2");
        instance().getMap(mapName).put(3, "value-3");

        assertRowsAnyOrder(
                "SELECT l.v, m.__key, m.this || '-s' " +
                        "FROM " + leftName + " l " +
                        "LEFT OUTER JOIN " + mapName + " m ON l.v = m.__key ",
                asList(
                        new Row(0, null, null),
                        new Row(null, null, null),
                        new Row(2, 2, "value-2-s")
                )
        );
    }

    @Test
    public void test_leftJoinOnPrimitiveKey() {
        String leftName = randomName();
        TestBatchSqlConnector.create(
                sqlService,
                leftName,
                singletonList("v"),
                singletonList(INT),
                asList(new String[]{"0"}, new String[]{null}, new String[]{"2"})
        );

        String mapName = randomName();
        instance().getMap(mapName).put(1, "value-1");
        instance().getMap(mapName).put(2, "value-2");
        instance().getMap(mapName).put(3, "value-3");

        assertRowsAnyOrder(
                "SELECT l.v, m.__key, m.this " +
                        "FROM " + leftName + " l " +
                        "LEFT JOIN " + mapName + " m ON l.v = m.__key",
                asList(
                        new Row(0, null, null),
                        new Row(null, null, null),
                        new Row(2, 2, "value-2")
                )
        );
    }

    @Test
    public void test_leftJoinNotOnPrimitiveKey() {
        String leftName = randomName();
        TestBatchSqlConnector.create(
                sqlService,
                leftName,
                singletonList("v"),
                singletonList(INT),
                asList(new String[]{"0"}, new String[]{null}, new String[]{"2"})
        );

        String mapName = randomName();
        instance().getMap(mapName).put("value-1", 1);
        instance().getMap(mapName).put("value-2", 2);
        instance().getMap(mapName).put("value-3", 3);

        assertRowsAnyOrder(
                "SELECT l.v, m.__key, m.this " +
                        "FROM " + leftName + " l " +
                        "LEFT JOIN " + mapName + " m ON l.v = m.this",
                asList(
                        new Row(0, null, null),
                        new Row(null, null, null),
                        new Row(2, "value-2", 2)
                )
        );
    }

    @Test
    public void test_leftJoinNotOnPrimitiveKey_withAdditionalCondition() {
        String leftName = randomName();
        TestBatchSqlConnector.create(
                sqlService,
                leftName,
                singletonList("v"),
                singletonList(INT),
                asList(new String[]{"0"}, new String[]{null}, new String[]{"2"})
        );

        String mapName = randomName();
        instance().getMap(mapName).put("value-1", 1);
        instance().getMap(mapName).put("value-2", 2);
        instance().getMap(mapName).put("value-3", 3);

        assertRowsAnyOrder(
                "SELECT l.v, m.__key, m.this " +
                        "FROM " + leftName + " l " +
                        "LEFT JOIN " + mapName + " m ON l.v = m.this and m.__key is null",
                asList(
                        new Row(0, null, null),
                        new Row(null, null, null),
                        new Row(2, null, null)
                )
        );
    }

    @Test
    public void test_leftJoinNotOnPrimitiveKey_multipleMatches() {
        String leftName = randomName();
        TestBatchSqlConnector.create(
                sqlService,
                leftName,
                singletonList("v"),
                singletonList(INT),
                asList(new String[]{"0"}, new String[]{null}, new String[]{"2"})
        );

        String mapName = randomName();
        instance().getMap(mapName).put("value-1", 1);
        instance().getMap(mapName).put("value-2", 2);
        instance().getMap(mapName).put("value-3", 2);

        assertRowsAnyOrder(
                "SELECT l.v, m.__key, m.this " +
                        "FROM " + leftName + " l " +
                        "LEFT JOIN " + mapName + " m ON l.v = m.this",
                asList(
                        new Row(0, null, null),
                        new Row(null, null, null),
                        new Row(2, "value-2", 2),
                        new Row(2, "value-3", 2)
                )
        );
    }

    @Test
    public void test_leftJoinNotOnPrimitiveKey_multipleMatches_additionalCondition() {
        String leftName = randomName();
        TestBatchSqlConnector.create(
                sqlService,
                leftName,
                singletonList("v"),
                singletonList(INT),
                asList(new String[]{"0"}, new String[]{null}, new String[]{"2"})
        );

        String mapName = randomName();
        instance().getMap(mapName).put("value-1", 1);
        instance().getMap(mapName).put("value-2", 2);
        instance().getMap(mapName).put("value-3", 2);

        assertRowsAnyOrder(
                "SELECT l.v, m.__key, m.this " +
                        "FROM " + leftName + " l " +
                        "LEFT JOIN " + mapName + " m ON l.v = m.this and m.__key='value-3'",
                asList(
                        new Row(0, null, null),
                        new Row(null, null, null),
                        new Row(2, "value-3", 2)
                )
        );
    }

    @Test
    public void test_leftJoinWithAlwaysTrueCondition() {
        String leftName = randomName();
        TestBatchSqlConnector.create(
                sqlService,
                leftName,
                singletonList("v"),
                singletonList(INT),
                asList(new String[]{"0"}, new String[]{null})
        );

        String mapName = randomName();
        instance().getMap(mapName).put("value-1", 1);
        instance().getMap(mapName).put("value-2", 2);
        instance().getMap(mapName).put("value-3", 3);

        assertRowsAnyOrder(
                "SELECT l.v, m.__key, m.this " +
                        "FROM " + leftName + " l " +
                        "LEFT JOIN " + mapName + " m ON 1 = 1",
                asList(
                        new Row(0, "value-1", 1),
                        new Row(0, "value-2", 2),
                        new Row(0, "value-3", 3),
                        new Row(null, "value-1", 1),
                        new Row(null, "value-2", 2),
                        new Row(null, "value-3", 3)
                )
        );
    }

    @Test
    public void test_leftJoinWithNonEquiJoin() {
        String leftName = randomName();
        TestBatchSqlConnector.create(
                sqlService,
                leftName,
                singletonList("v"),
                singletonList(INT),
                asList(new String[]{"0"}, new String[]{null}, new String[]{"2"}, new String[]{"3"})
        );

        String mapName = randomName();
        instance().getMap(mapName).put(1, "value-1");
        instance().getMap(mapName).put(2, "value-2");
        instance().getMap(mapName).put(3, "value-3");

        assertRowsAnyOrder(
                "SELECT l.v, m.__key, m.this " +
                        "FROM " + leftName + " l " +
                        "LEFT JOIN " + mapName + " m ON m.__key>l.v",
                asList(
                        new Row(0, 1, "value-1"),
                        new Row(0, 2, "value-2"),
                        new Row(0, 3, "value-3"),
                        new Row(null, null, null),
                        new Row(2, 3, "value-3"),
                        new Row(3, null, null)
                )
        );
    }

    @Test
    public void test_leftJoinWithNonEquiJoin_additionalCondition() {
        String leftName = randomName();
        TestBatchSqlConnector.create(
                sqlService,
                leftName,
                singletonList("v"),
                singletonList(INT),
                asList(new String[]{"0"}, new String[]{null}, new String[]{"2"}, new String[]{"3"})
        );

        String mapName = randomName();
        instance().getMap(mapName).put(1, "value-1");
        instance().getMap(mapName).put(2, "value-2");
        instance().getMap(mapName).put(3, "value-3");

        assertRowsAnyOrder(
                "SELECT l.v, m.__key, m.this " +
                        "FROM " + leftName + " l " +
                        "LEFT JOIN " + mapName + " m ON m.__key>l.v AND m.this IS NOT NULL",
                asList(
                        new Row(0, 1, "value-1"),
                        new Row(0, 2, "value-2"),
                        new Row(0, 3, "value-3"),
                        new Row(null, null, null),
                        new Row(2, 3, "value-3"),
                        new Row(3, null, null)

                )
        );
    }
}
