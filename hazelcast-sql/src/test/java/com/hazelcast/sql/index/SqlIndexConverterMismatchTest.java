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

package com.hazelcast.sql.index;

import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.support.expressions.ExpressionBiValue;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Simple test to verify the behavior in case of the converter type mismatch.
 * <p>
 * We put entries of different types to different members and observe that index lookup cannot be used due to mismatch.
 */
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlIndexConverterMismatchTest extends SqlTestSupport {

    private static final String MAP_NAME = "map";

    private final TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(2);
    private HazelcastInstance member1;
    private HazelcastInstance member2;
    private IMap<Integer, ExpressionBiValue> map;

    @Parameterized.Parameter
    public boolean composite;

    @Parameterized.Parameters(name = "composite:{0}")
    public static Collection<Object[]> parameters() {
        List<Object[]> res = new ArrayList<>();

        res.add(new Object[]{false});
        res.add(new Object[]{true});

        return res;
    }

    @Before
    public void before() {
        IndexConfig indexConfig = new IndexConfig().setName("index").setType(IndexType.SORTED).addAttribute("field1");

        if (composite) {
            indexConfig.addAttribute("field2");
        }

        Config config = getConfig();
        config.addMapConfig(new MapConfig().setName(MAP_NAME).setBackupCount(0).addIndexConfig(indexConfig));

        member1 = factory.newHazelcastInstance(config);
        member2 = factory.newHazelcastInstance(config);

        map = member1.getMap(MAP_NAME);
    }

    @SuppressWarnings("StatementWithEmptyBody")
    @Test
    public void testMismatch() {
        ExpressionBiValue value1 = new ExpressionBiValue.IntegerIntegerVal();
        value1.field1(10);
        value1.field2(10);

        ExpressionBiValue value2 = new ExpressionBiValue.StringIntegerVal();
        value2.field1("10");
        value2.field2(10);

        map.put(getLocalKey(member1, key -> key), value1);
        map.put(getLocalKey(member2, key -> key), value2);

        try {
            try (SqlResult result = member1.getSql().execute("SELECT key FROM " + MAP_NAME + " WHERE field1=1")) {
                for (SqlRow ignore : result) {
                    // No-op.
                }
            }

            fail("Must fail!");
        } catch (HazelcastSqlException e) {
            assertEquals(SqlErrorCode.INDEX_INVALID, e.getCode());
            assertEquals("Cannot use the index \"index\" of the IMap \"map\" because it has component \"field1\" of type VARCHAR, but INTEGER was expected", e.getMessage());
        }
    }

    @After
    public void after() {
        factory.shutdownAll();
    }
}
