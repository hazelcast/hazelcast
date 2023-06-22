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

package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.PartitioningAttributeConfig;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Arrays;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class PartitionPruningOptTest extends SqlTestSupport {
    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
    }

    @Before
    public void init() {
        instance().getConfig().addMapConfig(new MapConfig("test2").setPartitioningAttributeConfigs(Arrays.asList(
                new PartitioningAttributeConfig("comp1"),
                new PartitioningAttributeConfig("comp2")
        )));
        instance().getSql().execute("CREATE MAPPING test1 TYPE IMap OPTIONS ('keyFormat'='bigint', 'valueFormat'='varchar')");
        instance().getSql().execute("CREATE MAPPING test2 TYPE IMap OPTIONS ("
                + "'valueFormat'='varchar', "
                + "'keyFormat'='java', "
                + "'keyJavaClass'='" + KeyObj.class.getName() + "')");

        instance().getSql().execute("SINK INTO test1 VALUES (1, 'v1')");
        instance().getSql().execute("SINK INTO test1 VALUES (2, 'v2')");

        instance().getSql().execute("SINK INTO test2 VALUES (1, 1, 100, 'v1')");
        instance().getSql().execute("SINK INTO test2 VALUES (2, 2, 200, 'v2')");
        instance().getSql().execute("SINK INTO test2 VALUES (3, 3, 300, 'v3')");
        instance().getSql().execute("SINK INTO test2 VALUES (4, 4, 300, 'v3')");
        instance().getSql().execute("SINK INTO test2 VALUES (5, 5, 300, 'v3')");
        instance().getSql().execute("SINK INTO test2 VALUES (6, 6, 300, 'v3')");

        for (int i = 0; i < 5; i++) {
            System.out.println();
        }
        System.out.println("=====================================");
    }

    static final String[] QUERIES = new String[] {
            "SELECT * FROM test WHERE __key = 1",
            "SELECT * FROM test WHERE __key = 1 OR __key = 3",
            "SELECT * FROM test WHERE key_comp1 = 1 AND key_comp2 = 2", // strategy using (key_comp1, key_comp2) attribute tuple
            "SELECT t1.this, t2.this FROM t1 JOIN t2 ON t1.this = t2.this WHERE t1.__key = 1 AND t2.__key = 3",
            "SELECT t1.this, t2.this FROM t1 JOIN t2 ON t1.__key = t2.__key WHERE t1.__key = 1 ",
            "SELECT * FROM test WHERE __key IN (1,3,5)",
            "SELECT * FROM test WHERE key_comp1 IN (1,3,5) OR key_comp2 IN (3,4,5)", // sum of affected partitions from both conditions
            "SELECT * FROM test WHERE key_comp1 = 1", // Wrong case, key_comp2 is missing in the condition
            "SELECT * FROM test WHERE this = 1", // Wrong case, no key filter
            "SELECT * FROM t1 JOIN t2 ON t1.__key = t2.__key WHERE t1.__key = t2.this", // Wrong case, impossible to know partitions without querying the underlying data first
            "SELECT * FROM test WHERE __key = 1 OR this = 1" // Wrong case, this = 1 makes this query unbounded partition-wise
    };

    @Test
    public void test_singleKeyConditions() {
        executeWithLog("SELECT * FROM test1 WHERE __key = 1");
        executeWithLog("SELECT * FROM test1 WHERE __key = ?", 1);
        executeWithLog("SELECT * FROM test1 WHERE __key = 1 AND this = 'v1'");
        executeWithLog("SELECT * FROM test2 WHERE comp1 = 1 AND comp2 = 1");
        executeWithLog("SELECT * FROM test2 WHERE comp1 IN (1) AND comp2 IN (1)");
    }

    @Test
    public void test_multipleKeyConditions() {
        executeWithLog("SELECT * FROM test1 WHERE __key IN(1, 2, 3)");
        executeWithLog("SELECT * FROM test1 WHERE __key IN(?, ?, ?)", 1, 2, 3);
        executeWithLog("SELECT * FROM test1 WHERE __key = 1 OR __key = 2");
        executeWithLog("SELECT * FROM test2 WHERE comp1 IN (1, ?, 3)AND comp2 IN(4, ?, 6)", 2, 5);
        executeWithLog("SELECT * FROM test1 WHERE __key BETWEEN 1 AND 10");
        executeWithLog("SELECT * FROM test2 WHERE comp1 BETWEEN 1 AND 3 AND comp2 BETWEEN 4 AND 6");
    }

    @Test
    public void test_joinsWithKeyFilters() {
//        executeWithLog("SELECT t1.this, t2.this FROM test1 AS t1 JOIN test1 AS t2 ON t1.__key = t2.__key WHERE t1.__key = 1 AND t2.__key = 1");
//        executeWithLog("SELECT t1.this, t2.this FROM test1 AS t1 JOIN test2 AS t2 ON t1.__key = t2.comp1 WHERE t1.__key = ? AND t2.comp1 = ? AND t2.comp2 = ?", 1, 1, 1);
//        executeWithLog("SELECT t1.__key FROM test1 AS t1 JOIN test2 AS t2 ON t1.__key = t2.comp1 LEFT OUTER JOIN test2 AS t3 ON t2.__key = t3.__key WHERE t1.__key = ? AND t2.comp1 = ? AND t2.comp2 = ? GROUP BY t1.__key ORDER BY t1.__key DESC LIMIT 5", 1, 1, 1);
        executeWithLog("SELECT t1.this, t2.this, t3.this, t1.__key FROM test1 AS t1 JOIN test2 AS t2 ON t1.__key = t2.comp1 LEFT OUTER JOIN test2 AS t3 ON t2.__key = t3.__key WHERE ? = t1.__key AND t2.comp1 = ? AND t2.comp2 = ? AND t3.__key = ?", 1, 1, 1, 1);
    }

    @Test
    public void test() {
        executeWithLog("SELECT * FROM test1 WHERE __key = ?", 1);
//        executeWithLog("SELECT * FROM test1 WHERE (__key = ? OR __key = ?) AND this = 'v1'", 1, 2);
//        executeWithLog("SELECT * FROM test1 WHERE __key = ? AND this = 'v1' LIMIT 1", 1);
//        executeWithLog("SELECT * FROM test1 WHERE __key = ? AND this = 'v1' ORDER BY __key LIMIT 1", 1);
//        executeWithLog("SELECT __key FROM test1 WHERE __key = ? AND this = 'v1' GROUP BY __key", 1);
    }


    @Test
    public void test_aggregations() {
        executeWithLog("SELECT count(*) FROM test1 WHERE __key BETWEEN 1 AND 3");
        executeWithLog("SELECT 1 FROM test1 WHERE MIN(__key) = 1 AND MAX(__key) < 3 GROUP BY __key");
    }

    public void executeWithLog(String sql, Object... args) {
        System.out.println("-------------------------------------------------------------------------------------------");
        System.out.println("Executing: " + sql);
        instance().getSql().execute(sql, args);
    }


    @Test
    public void test_something() {
//        instance().getSql().execute("SELECT * FROM test WHERE __key = ? AND this = ''", 113);
//        instance().getSql().execute("SELECT * FROM test2 WHERE comp2 = ? AND comp3 = ?", 113, 113);
        instance().getSql().execute("SELECT * FROM test2 WHERE comp1 = 113 AND comp2 = ?", 113);
    }

    public static class KeyObj implements Serializable {
        public Long comp1;
        public Long comp2;
        public Long comp3;

        public KeyObj() {

        }

        public KeyObj(final Long comp1, final Long comp2, final Long comp3) {
            this.comp1 = comp1;
            this.comp2 = comp2;
            this.comp3 = comp3;
        }
    }
}
