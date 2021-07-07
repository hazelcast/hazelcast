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
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.exec.scan.index.IndexEqualsFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilterValue;
import com.hazelcast.sql.impl.exec.scan.index.IndexRangeFilter;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.rel.RelFieldCollation;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.BiPredicate;

import static com.hazelcast.jet.TestContextSupport.adaptSupplier;
import static com.hazelcast.jet.sql.impl.ExpressionUtil.comparisonFn;
import static com.hazelcast.jet.sql.impl.SimpleExpressionEvalContext.SQL_ARGUMENTS_KEY_NAME;
import static com.hazelcast.sql.impl.SqlTestSupport.valuePath;
import static com.hazelcast.sql.impl.expression.ColumnExpression.create;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

@SuppressWarnings("rawtypes")
@RunWith(Parameterized.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapIndexScanPTest extends SimpleTestInClusterSupport {

    @Parameterized.Parameters(name = "count:{0}")
    public static Collection<Integer> parameters() {
        return asList(100, 10_000);
    }

    @Parameterized.Parameter(0)
    public int count;

    private IMap<Integer, Person> map;

    public static final BiPredicate<List<?>, List<?>> LENIENT_SAME_ITEMS_IN_ORDER =
            (expected, actual) -> {
                if (expected.size() != actual.size()) { // shortcut
                    return false;
                }
                List<Object[]> expectedList = (List<Object[]>) expected;
                List<Object[]> actualList = (List<Object[]>) actual;
                for (int i = 0; i < expectedList.size(); i++) {
                    if (!Arrays.equals(expectedList.get(i), actualList.get(i))) {
                        return false;
                    }
                }
                return true;
            };

    @BeforeClass
    public static void setUp() {
        initialize(1, null);
    }

    @Before
    public void before() {
        map = instance().getMap(randomMapName());
    }

    // We also don't test full hash index scan, because such plan aren't allowed to be created.
    @Test
    public void test_pointLookup_hashed() {
        List<Object[]> expected = new ArrayList<>();
        for (int i = count; i > 0; i--) {
            map.put(i, new Person("value-" + i, i));
        }

        expected.add(new Object[]{(5), "value-5", 5});

        IndexConfig indexConfig = new IndexConfig(IndexType.HASH, "age").setName(randomName());
        map.addIndex(indexConfig);

        IndexFilter filter = new IndexEqualsFilter(intValue(5));
        List<Expression<?>> projections = asList(create(0, INT), create(1, VARCHAR), create(2, INT));

        MapIndexScanMetadata scanMetadata = new MapIndexScanMetadata(
                map.getName(),
                indexConfig.getName(),
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                Arrays.asList(QueryPath.KEY_PATH, valuePath("name"), valuePath("age")),
                Arrays.asList(INT, VARCHAR, INT),
                filter,
                projections,
                projections,
                null,
                null,
                false
        );

        TestSupport
                .verifyProcessor(adaptSupplier(MapIndexScanP.readMapIndexSupplier(scanMetadata)))
                .hazelcastInstance(instance())
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .outputChecker(LENIENT_SAME_ITEMS_IN_ORDER)
                .disableSnapshots()
                .disableProgressAssertion()
                .expectOutput(expected);
    }

    @Test
    public void test_fullScanAsc_sorted() {
        List<Object[]> expected = new ArrayList<>();
        for (int i = count; i > 0; i--) {
            map.put(i, new Person("value-" + i, i));
            expected.add(new Object[]{(count - i + 1), "value-" + (count - i + 1), (count - i + 1)});
        }

        IndexConfig indexConfig = new IndexConfig(IndexType.SORTED, "age").setName(randomName());
        map.addIndex(indexConfig);

        IndexFilter filter = new IndexRangeFilter(null, true, null, true);
        List<Expression<?>> projections = asList(create(0, INT), create(1, VARCHAR), create(2, INT));

        MapIndexScanMetadata scanMetadata = new MapIndexScanMetadata(
                map.getName(),
                indexConfig.getName(),
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                Arrays.asList(QueryPath.KEY_PATH, valuePath("name"), valuePath("age")),
                Arrays.asList(INT, VARCHAR, INT),
                filter,
                projections,
                projections,
                null,
                comparisonFn(singletonList(new FieldCollation(new RelFieldCollation(2)))),
                false
        );

        TestSupport
                .verifyProcessor(adaptSupplier(MapIndexScanP.readMapIndexSupplier(scanMetadata)))
                .hazelcastInstance(instance())
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .outputChecker(LENIENT_SAME_ITEMS_IN_ORDER)
                .disableSnapshots()
                .disableProgressAssertion()
                .expectOutput(expected);
    }

    @Test
    public void test_fullScanDesc_sorted() {
        List<Object[]> expected = new ArrayList<>();
        for (int i = 0; i <= count; i++) {
            map.put(i, new Person("value-" + i, i));
            expected.add(new Object[]{(count - i), "value-" + (count - i), (count - i)});
        }

        IndexConfig indexConfig = new IndexConfig(IndexType.SORTED, "age").setName(randomName());
        map.addIndex(indexConfig);

        IndexFilter filter = new IndexRangeFilter(null, true, null, true);
        List<Expression<?>> projections = asList(create(0, INT), create(1, VARCHAR), create(2, INT));

        MapIndexScanMetadata scanMetadata = new MapIndexScanMetadata(
                map.getName(),
                indexConfig.getName(),
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                Arrays.asList(QueryPath.KEY_PATH, valuePath("name"), valuePath("age")),
                Arrays.asList(INT, VARCHAR, INT),
                filter,
                projections,
                projections,
                null,
                comparisonFn(singletonList(new FieldCollation(new RelFieldCollation(2)))),
                true
        );

        TestSupport
                .verifyProcessor(adaptSupplier(MapIndexScanP.readMapIndexSupplier(scanMetadata)))
                .hazelcastInstance(instance())
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .outputChecker(LENIENT_SAME_ITEMS_IN_ORDER)
                .disableSnapshots()
                .disableProgressAssertion()
                .expectOutput(expected);
    }

    @Test
    public void test_whenFilterExistsWithoutSpecificProjection_sorted() {
        List<Object[]> expected = new ArrayList<>();
        for (int i = count; i > 0; i--) {
            map.put(i, new Person("value-" + i, i));
            if (i > count / 2) {
                expected.add(new Object[]{(count - i + 1), "value-" + (count - i + 1), (count - i + 1)});
            }
        }

        IndexConfig indexConfig = new IndexConfig(IndexType.SORTED, "age").setName(randomName());
        map.addIndex(indexConfig);

        IndexFilter filter = new IndexRangeFilter(intValue(0), true, intValue(count / 2), true);
        List<Expression<?>> projections = asList(create(0, INT), create(1, VARCHAR), create(2, INT));

        MapIndexScanMetadata scanMetadata = new MapIndexScanMetadata(
                map.getName(),
                indexConfig.getName(),
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                Arrays.asList(QueryPath.KEY_PATH, valuePath("name"), valuePath("age")),
                Arrays.asList(INT, VARCHAR, INT),
                filter,
                projections,
                projections,
                null,
                comparisonFn(singletonList(new FieldCollation(new RelFieldCollation(2)))),
                false
        );

        TestSupport
                .verifyProcessor(adaptSupplier(MapIndexScanP.readMapIndexSupplier(scanMetadata)))
                .hazelcastInstance(instance())
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .outputChecker(LENIENT_SAME_ITEMS_IN_ORDER)
                .disableSnapshots()
                .disableProgressAssertion()
                .expectOutput(expected);
    }

    @Test
    public void test_whenFilterAndSpecificProjectionExists_sorted() {
        List<Object[]> expected = new ArrayList<>();
        for (int i = count; i > 0; i--) {
            map.put(i, new Person("value-" + i, i));
            if (i > count / 2) {
                expected.add(new Object[]{(count - i + 1), "value-" + (count - i + 1), (count - i + 1)});
            }
        }

        IndexConfig indexConfig = new IndexConfig(IndexType.SORTED, "age");
        indexConfig.setName(randomName());
        map.addIndex(indexConfig);

        IndexFilter filter = new IndexRangeFilter(intValue(0), true, intValue(count / 2), true);
        List<Expression<?>> projection = asList(create(0, INT), create(1, VARCHAR), create(2, INT));

        MapIndexScanMetadata scanMetadata = new MapIndexScanMetadata(
                map.getName(),
                indexConfig.getName(),
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                Arrays.asList(QueryPath.KEY_PATH, valuePath("name"), valuePath("age")),
                Arrays.asList(INT, VARCHAR, INT),
                filter,
                projection,
                projection,
                null,
                comparisonFn(singletonList(new FieldCollation(new RelFieldCollation(0)))),
                false
        );

        TestSupport
                .verifyProcessor(adaptSupplier(MapIndexScanP.readMapIndexSupplier(scanMetadata)))
                .hazelcastInstance(instance())
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .outputChecker(LENIENT_SAME_ITEMS_IN_ORDER)
                .disableSnapshots()
                .disableProgressAssertion()
                .expectOutput(expected);
    }

    static class Person implements DataSerializable {
        private String name;
        private int age;

        Person() {
            // no op.
        }

        public String getName() {
            return name;
        }

        public int getAge() {
            return age;
        }

        Person(String name, int age) {
            this.name = name;
            this.age = age;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeString(name);
            out.writeInt(age);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            this.name = in.readString();
            this.age = in.readInt();
        }
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

}
