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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.opt.FieldCollation;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexRangeFilter;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.plan.node.MapIndexScanMetadata;
import org.apache.calcite.rel.RelFieldCollation;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static com.hazelcast.jet.TestContextSupport.adaptSupplier;
import static com.hazelcast.jet.sql.SqlTestSupport.assertRowsOrdered;
import static com.hazelcast.jet.sql.impl.ExpressionUtil.comparisonFn;
import static com.hazelcast.jet.sql.impl.SimpleExpressionEvalContext.SQL_ARGUMENTS_KEY_NAME;
import static com.hazelcast.jet.sql.impl.connector.map.MapIndexScanUtils.intValue;
import static com.hazelcast.sql.impl.SqlTestSupport.valuePath;
import static com.hazelcast.sql.impl.expression.ColumnExpression.create;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class MapIndexScanPMigrationStressTest extends SimpleTestInClusterSupport {
    static final int ITEM_COUNT = 500;
    static final String MAP_NAME = "map";

    private IMap<Integer, Integer> map;

    @BeforeClass
    public static void setUpClass() {
        initialize(3, smallInstanceConfig());
    }

    @Before
    public void before() {
        map = instance().getMap(MAP_NAME);
    }

    @Ignore
    @Test
    public void test() {
        List<Object[]> expected = new ArrayList<>();
        for (int i = ITEM_COUNT; i >= 0; i--) {
            map.put(i, i);
            expected.add(new Object[]{ITEM_COUNT - i, ITEM_COUNT - i});
        }

        IndexConfig indexConfig = new IndexConfig(IndexType.SORTED, "this").setName(randomName());
        map.addIndex(indexConfig);

        IndexFilter filter = new IndexRangeFilter(intValue(0), true, intValue(ITEM_COUNT), true);
        List<Expression<?>> projections = asList(create(0, INT), create(1, INT));

        MapIndexScanMetadata scanMetadata = new MapIndexScanMetadata(
                map.getName(),
                indexConfig.getName(),
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                Arrays.asList(QueryPath.KEY_PATH, valuePath("this")),
                Arrays.asList(INT, INT),
                filter,
                projections,
                projections,
                null,
                comparisonFn(singletonList(new FieldCollation(new RelFieldCollation(1))))
        );

//        MutatorThread mutator = new MutatorThread(instances());
//        mutator.start();

        TestSupport
                .verifyProcessor(adaptSupplier(MapIndexScanP.readMapIndexSupplier(scanMetadata)))
                .hazelcastInstance(instance())
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .outputChecker(MapIndexScanPTest.LENIENT_SAME_ITEMS_IN_ORDER)
                .disableSnapshots()
                .disableProgressAssertion()
                .expectOutput(expected);

//        try {
//            mutator.join();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
    }

    @Test
    public void testWithSql() {
        List<SqlTestSupport.Row> expected = new ArrayList<>();
        for (int i = ITEM_COUNT; i >= 0; i--) {
            map.put(i, i);
            expected.add(new SqlTestSupport.Row(ITEM_COUNT - i, ITEM_COUNT - i));
        }

        IndexConfig indexConfig = new IndexConfig(IndexType.SORTED, "this").setName(randomName());
        map.addIndex(indexConfig);

        MutatorThread mutator = new MutatorThread(instances());
//        mutator.start();

        assertRowsOrdered("SELECT * FROM " + MAP_NAME, expected);

//        try {
//            mutator.join();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
    }

    private static class MutatorThread extends Thread {
        private final HazelcastInstance[] instances;
        private final Random random;

        MutatorThread(HazelcastInstance[] instances) {
            super();
            this.instances = instances;
            random = new Random(System.currentTimeMillis());
        }

        @Override
        public void run() {
            int instanceReplace = random.nextInt(instances.length);
            instances[instanceReplace] = factory().newHazelcastInstance(smallInstanceConfig());
            try {
                Thread.sleep(2000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
