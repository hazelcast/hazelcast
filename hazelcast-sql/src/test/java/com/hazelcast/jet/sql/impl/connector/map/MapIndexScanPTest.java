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
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.sql.impl.opt.FieldCollation;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.impl.exec.scan.MapIndexScanMetadata;
import com.hazelcast.sql.impl.exec.scan.index.IndexEqualsFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilterValue;
import com.hazelcast.sql.impl.exec.scan.index.IndexRangeFilter;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.FunctionalPredicateExpression;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.rel.RelFieldCollation;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.BiPredicate;

import static com.hazelcast.jet.TestContextSupport.adaptSupplier;
import static com.hazelcast.jet.sql.SqlTestSupport.jetRow;
import static com.hazelcast.jet.sql.impl.ExpressionUtil.comparisonFn;
import static com.hazelcast.sql.impl.expression.ColumnExpression.create;
import static com.hazelcast.sql.impl.expression.ExpressionEvalContext.SQL_ARGUMENTS_KEY_NAME;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

@SuppressWarnings("rawtypes")
@RunWith(HazelcastParametrizedRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapIndexScanPTest extends SimpleTestInClusterSupport {

    @Parameterized.Parameters(name = "count:{0}")
    public static Collection<Integer> parameters() {
        return asList(1_000, 50_000);
    }

    @Parameterized.Parameter()
    public int count;

    private IMap<Integer, Person> map;

    @SuppressWarnings("unchecked")
    private static final BiPredicate<List<?>, List<?>> LENIENT_SAME_ITEMS_IN_ORDER =
            (expected, actual) -> {
                if (expected.size() != actual.size()) {
                    return false;
                }
                List<JetSqlRow> expectedList = (List<JetSqlRow>) expected;
                List<JetSqlRow> actualList = (List<JetSqlRow>) actual;
                for (int i = 0; i < expectedList.size(); i++) {
                    if (!Objects.equals(expectedList.get(i), actualList.get(i))) {
                        return false;
                    }
                }
                return true;
            };

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
    }

    @Before
    public void before() {
        map = instance().getMap(randomMapName());
    }

    // We also don't test full hash index scan, because such plan aren't allowed to be created.
    @Test
    public void test_pointLookup_hashed() {
        List<JetSqlRow> expected = new ArrayList<>();
        for (int i = count; i > 0; i--) {
            map.put(i, new Person("value-" + i, i));
        }
        expected.add(jetRow((5), "value-5", 5));

        IndexConfig indexConfig = new IndexConfig(IndexType.HASH, "age").setName(randomName());
        map.addIndex(indexConfig);

        IndexFilter filter = new IndexEqualsFilter(intValue(5));
        MapIndexScanMetadata metadata = metadata(indexConfig.getName(), filter, -1, false);

        TestSupport
                .verifyProcessor(adaptSupplier(MapIndexScanP.readMapIndexSupplier(metadata)))
                .hazelcastInstance(instance())
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .outputChecker(LENIENT_SAME_ITEMS_IN_ORDER)
                .disableSnapshots()
                .disableProgressAssertion()
                .expectOutput(expected);
    }

    @Test
    public void test_fullScanAsc_sorted() {
        List<JetSqlRow> expected = new ArrayList<>();
        for (int i = count; i > 0; i--) {
            map.put(i, new Person("value-" + i, i));
            expected.add(jetRow((count - i + 1), "value-" + (count - i + 1), (count - i + 1)));
        }

        IndexConfig indexConfig = new IndexConfig(IndexType.SORTED, "age").setName(randomName());
        map.addIndex(indexConfig);

        IndexFilter filter = new IndexRangeFilter(null, true, null, true);
        MapIndexScanMetadata metadata = metadata(indexConfig.getName(), filter, 2, false);

        TestSupport
                .verifyProcessor(adaptSupplier(MapIndexScanP.readMapIndexSupplier(metadata)))
                .hazelcastInstance(instance())
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .outputChecker(LENIENT_SAME_ITEMS_IN_ORDER)
                .disableSnapshots()
                .disableProgressAssertion()
                .expectOutput(expected);
    }

    @Test
    public void test_fullScanDesc_sorted() {
        List<JetSqlRow> expected = new ArrayList<>();
        for (int i = 0; i <= count; i++) {
            map.put(i, new Person("value-" + i, i));
            expected.add(jetRow((count - i), "value-" + (count - i), (count - i)));
        }

        IndexConfig indexConfig = new IndexConfig(IndexType.SORTED, "age").setName(randomName());
        map.addIndex(indexConfig);

        IndexFilter filter = new IndexRangeFilter(null, true, null, true);
        MapIndexScanMetadata metadata = metadata(indexConfig.getName(), filter, 2, true);

        TestSupport
                .verifyProcessor(adaptSupplier(MapIndexScanP.readMapIndexSupplier(metadata)))
                .hazelcastInstance(instance())
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .outputChecker(LENIENT_SAME_ITEMS_IN_ORDER)
                .disableSnapshots()
                .disableProgressAssertion()
                .expectOutput(expected);
    }

    @Test
    public void test_whenFilterExistsWithoutSpecificProjection_sorted() {
        List<JetSqlRow> expected = new ArrayList<>();
        for (int i = count; i > 0; i--) {
            map.put(i, new Person("value-" + i, i));
            if (i > count / 2) {
                expected.add(jetRow((count - i + 1), "value-" + (count - i + 1), (count - i + 1)));
            }
        }

        IndexConfig indexConfig = new IndexConfig(IndexType.SORTED, "age").setName(randomName());
        map.addIndex(indexConfig);

        IndexFilter filter = new IndexRangeFilter(intValue(0), true, intValue(count / 2), true);
        MapIndexScanMetadata metadata = metadata(indexConfig.getName(), filter, 2, false);

        TestSupport
                .verifyProcessor(adaptSupplier(MapIndexScanP.readMapIndexSupplier(metadata)))
                .hazelcastInstance(instance())
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .outputChecker(LENIENT_SAME_ITEMS_IN_ORDER)
                .disableSnapshots()
                .disableProgressAssertion()
                .expectOutput(expected);
    }

    @Test
    public void test_whenBothFiltersAndSpecificProjectionExists_sorted() {
        List<JetSqlRow> expected = new ArrayList<>();
        for (int i = count; i > 0; i--) {
            map.put(i, new Person("value-" + i, i));
            if (i > count / 2) {
                if (i % 2 == 1) {
                    expected.add(jetRow((count - i + 1), "value-" + (count - i + 1), (count - i + 1)));
                }
            }
        }

        IndexConfig indexConfig = new IndexConfig(IndexType.SORTED, "age").setName(randomName());
        map.addIndex(indexConfig);

        Expression<Boolean> remainingFilter = new FunctionalPredicateExpression(row -> {
            int value = row.get(0);
            return value % 2 == 0;
        });
        IndexFilter filter = new IndexRangeFilter(intValue(0), true, intValue(count / 2), true);
        MapIndexScanMetadata metadata = metadata(indexConfig.getName(), filter, remainingFilter, 0, false);

        TestSupport
                .verifyProcessor(adaptSupplier(MapIndexScanP.readMapIndexSupplier(metadata)))
                .hazelcastInstance(instance())
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .outputChecker(LENIENT_SAME_ITEMS_IN_ORDER)
                .disableSnapshots()
                .disableProgressAssertion()
                .expectOutput(expected);
    }

    @Test
    public void test_whenFilterAndSpecificProjectionExists_sorted() {
        List<JetSqlRow> expected = new ArrayList<>();
        for (int i = count; i > 0; i--) {
            map.put(i, new Person("value-" + i, i));
            if (i > count / 2) {
                expected.add(jetRow((count - i + 1), "value-" + (count - i + 1), (count - i + 1)));
            }
        }

        IndexConfig indexConfig = new IndexConfig(IndexType.SORTED, "age").setName(randomName());
        map.addIndex(indexConfig);

        IndexFilter filter = new IndexRangeFilter(intValue(0), true, intValue(count / 2), true);
        MapIndexScanMetadata metadata = metadata(indexConfig.getName(), filter, 0, false);

        TestSupport
                .verifyProcessor(adaptSupplier(MapIndexScanP.readMapIndexSupplier(metadata)))
                .hazelcastInstance(instance())
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .outputChecker(LENIENT_SAME_ITEMS_IN_ORDER)
                .disableSnapshots()
                .disableProgressAssertion()
                .expectOutput(expected);
    }

    private MapIndexScanMetadata metadata(String indexName, IndexFilter filter, int fieldIndex, boolean descending) {
        return metadata(indexName, filter, null, fieldIndex, descending);
    }

    private MapIndexScanMetadata metadata(
            String indexName,
            IndexFilter filter,
            Expression<Boolean> reminderFilter,
            int fieldIndex,
            boolean descending) {
        return new MapIndexScanMetadata(
                map.getName(),
                indexName,
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                Arrays.asList(QueryPath.KEY_PATH, valuePath("name"), valuePath("age")),
                Arrays.asList(INT, VARCHAR, INT),
                filter,
                asList(create(0, INT), create(1, VARCHAR), create(2, INT)),
                reminderFilter,
                fieldIndex == -1 ? null : comparisonFn(singletonList(new FieldCollation(new RelFieldCollation(fieldIndex)))),
                descending
        );
    }

    private static QueryPath valuePath(String path) {
        return path(path, false);
    }

    private static QueryPath path(String path, boolean key) {
        return new QueryPath(path, key);
    }

    private static IndexFilterValue intValue(Integer value) {
        return intValue(value, false);
    }

    private static IndexFilterValue intValue(Integer value, boolean allowNull) {
        return new IndexFilterValue(
                singletonList(constant(value, QueryDataType.INT)),
                singletonList(allowNull)
        );
    }

    private static ConstantExpression constant(Object value, QueryDataType type) {
        return ConstantExpression.create(value, type);
    }

    public static class Person implements Serializable {

        public String name;
        public int age;

        @SuppressWarnings("unused")
        private Person() {
        }

        private Person(String name, int age) {
            this.name = name;
            this.age = age;
        }
    }
}
