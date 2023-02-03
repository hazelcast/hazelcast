/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.jet.sql;

import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test for https://github.com/hazelcast/hazelcast/pull/22091
 */
@Category({QuickTest.class, ParallelJVMTest.class})
@SuppressWarnings("checkstyle:RedundantModifier")
public class SqlViewPlanInvalidationTest extends SqlTestSupport {
    public static final String FIRST_MAP_NAME = "m_one";
    public static final String SECOND_MAP_NAME = "m_two";
    public static final int FIRST_MAP_VALUE = 1;
    public static final int SECOND_MAP_VALUE = 2;
    public static final String FIRST_LITERAL = "hello";
    public static final String SECOND_LITERAL = "world";

    @BeforeClass
    public static void initialize() {
        // we use 1 member so that all queries use the same plan cache
        initialize(1, null);
    }

    @Test
    public void when_changingViewDefinitionWithLiteral_then_newViewDefinitionIsUsed() {
        instance().getSql().execute("CREATE OR REPLACE VIEW v1 AS SELECT '" + FIRST_LITERAL + "'");
        assertRowsAnyOrder("SELECT * FROM v1", rows(1, FIRST_LITERAL));
        instance().getSql().execute("CREATE OR REPLACE VIEW v1 AS SELECT '" + SECOND_LITERAL + "'");
        assertTrueEventually(() ->
                assertRowsAnyOrder("SELECT * FROM v1", rows(1, SECOND_LITERAL)));
    }

    @Test
    public void when_changingViewDefinitionWithIMap_then_newViewDefinitionIsUsed() {
        instance().getMap(FIRST_MAP_NAME).put("key", FIRST_MAP_VALUE);
        instance().getMap(SECOND_MAP_NAME).put("key", SECOND_MAP_VALUE);
        createMapping(FIRST_MAP_NAME, String.class, Integer.class);
        createMapping(SECOND_MAP_NAME, String.class, Integer.class);

        instance().getSql().execute("CREATE OR REPLACE VIEW v1 AS SELECT this from " + FIRST_MAP_NAME);
        assertRowsAnyOrder("SELECT * FROM v1", rows(1, FIRST_MAP_VALUE));
        instance().getSql().execute("CREATE OR REPLACE VIEW v1 AS SELECT this from " + SECOND_MAP_NAME);
        assertTrueEventually(() ->
                assertRowsAnyOrder("SELECT * FROM v1", rows(1, SECOND_MAP_VALUE)));
    }

    @Test
    public void when_changingInnerViewDefinitionWithNestedViews_then_newViewDefinitionIsUsed() {
        instance().getSql().execute("CREATE OR REPLACE VIEW v1 AS SELECT '" + FIRST_LITERAL + "'");
        instance().getSql().execute("CREATE OR REPLACE VIEW v2 AS SELECT * FROM v1");
        assertRowsAnyOrder("SELECT * FROM v2", rows(1, FIRST_LITERAL));
        instance().getSql().execute("CREATE OR REPLACE VIEW v1 AS SELECT '" + SECOND_LITERAL + "'");
        assertTrueEventually(() ->
                assertRowsAnyOrder("SELECT * FROM v2", rows(1, SECOND_LITERAL)));
    }

    @Test
    public void when_changingOuterViewDefinitionWithNestedViews_then_newViewDefinitionIsUsed() {
        instance().getSql().execute("CREATE OR REPLACE VIEW v1 AS SELECT '" + FIRST_LITERAL + "'");
        instance().getSql().execute("CREATE OR REPLACE VIEW v2 AS SELECT '" + SECOND_LITERAL + "'");
        instance().getSql().execute("CREATE OR REPLACE VIEW v3 AS SELECT * FROM v1");
        assertRowsAnyOrder("SELECT * FROM v3", rows(1, FIRST_LITERAL));
        instance().getSql().execute("CREATE OR REPLACE VIEW v3 AS SELECT * FROM v2");
        assertTrueEventually(() ->
                assertRowsAnyOrder("SELECT * FROM v2", rows(1, SECOND_LITERAL)));
    }
}
