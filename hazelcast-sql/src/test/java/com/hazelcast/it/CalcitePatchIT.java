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

package com.hazelcast.it;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;
import static org.assertj.core.api.Assertions.assertThat;

@Category(QuickTest.class)
public class CalcitePatchIT {

    private HazelcastInstance instance;

    @Before
    public void setUp() {
        instance = Hazelcast.newHazelcastInstance(HazelcastTestSupport.smallInstanceConfig());

    }

    @After
    public void tearDown() {
        instance.shutdown();
    }

    @Test
    @Category(QuickTest.class)
    // ensures HZ org.apache.calcite.linq4j.tree.ConstantExpression is picked up instead of Calcite one
    // https://issues.apache.org/jira/browse/CALCITE-4532
    public void verify_calcite_4532() {
        try (SqlResult result = instance.getSql().execute("SELECT CAST(" + Long.MAX_VALUE + " AS OBJECT) FROM (VALUES(1))")) {
            assertThat(stream(spliteratorUnknownSize(result.iterator(), ORDERED), false).map(row -> row.getObject(0)))
                    .containsExactly(Long.MAX_VALUE);
        }
    }
}
