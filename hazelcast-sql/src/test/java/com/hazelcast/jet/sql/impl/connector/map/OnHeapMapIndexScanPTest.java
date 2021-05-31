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

import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.exec.scan.index.IndexEqualsFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilterValue;
import com.hazelcast.sql.impl.exec.scan.index.IndexInFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexRangeFilter;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.ConstantPredicateExpression;
import com.hazelcast.sql.impl.expression.math.MultiplyFunction;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.plan.node.MapIndexScanMetadata;
import com.hazelcast.sql.impl.plan.node.MapScanMetadata;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.BiPredicate;

import static com.hazelcast.jet.TestContextSupport.adaptSupplier;
import static com.hazelcast.jet.sql.impl.SimpleExpressionEvalContext.SQL_ARGUMENTS_KEY_NAME;
import static com.hazelcast.sql.impl.SqlTestSupport.valuePath;
import static com.hazelcast.sql.impl.calcite.opt.physical.index.JetIndexResolver.composeFilter;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

@SuppressWarnings("rawtypes")
@RunWith(Parameterized.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OnHeapMapIndexScanPTest extends SimpleTestInClusterSupport {

    @Parameterized.Parameters(name = "count:{0}")
    public static Collection<Integer> parameters() {
        return asList(10, 25_000);
    }

    @Parameterized.Parameter(0)
    public int count;

    private IMap<Integer, Person> map;

    public static final BiPredicate<List<?>, List<?>> LENIENT_SAME_ITEMS_ANY_ORDER =
            (expected, actual) -> {
                if (expected.size() != actual.size()) { // shortcut
                    return false;
                }
                List<Object[]> expectedList = (List<Object[]>) expected;
                List<Object[]> actualList = (List<Object[]>) actual;
                expectedList.sort(Comparator.comparingInt((Object[] o) -> (int) o[0]));
                actualList.sort(Comparator.comparingInt((Object[] o) -> (int) o[0]));
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

    @Test
    public void test_whenNoFilterAndNoSpecificProjection() {
        List<Object[]> expected = new ArrayList<>();
        for (int i = count; i > 0; i--) {
            map.put(i, new Person("value-" + i, i));
            expected.add(new Object[]{(count - i + 1), "value-" + (count - i + 1), (count - i + 1)});
        }

        IndexConfig indexConfig = new IndexConfig(IndexType.SORTED, "name");
        indexConfig.setName(randomName());
        map.addIndex(indexConfig);

        MapScanMetadata scanMetadata = new MapScanMetadata(
                map.getName(),
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                Arrays.asList(QueryPath.KEY_PATH, valuePath("this.name"), valuePath("this.age")),
                Arrays.asList(QueryDataType.INT, VARCHAR, QueryDataType.INT),
                asList(
                        ColumnExpression.create(0, INT),
                        ColumnExpression.create(1, VARCHAR),
                        ColumnExpression.create(2, INT)
                ),
                new ConstantPredicateExpression(true)
        );

        MapIndexScanMetadata indexScanMetadata = new MapIndexScanMetadata(
                scanMetadata,
                indexConfig.getName(),
                1,
                null,
                emptyList(),
                emptyList()
        );

        TestSupport
                .verifyProcessor(adaptSupplier(OnHeapMapIndexScanP.onHeapMapIndexScanP(indexScanMetadata)))
                .jetInstance(instance())
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .outputChecker(LENIENT_SAME_ITEMS_ANY_ORDER)
                .disableSnapshots()
                .disableProgressAssertion()
                .expectOutput(expected);
    }

    @Test
    public void test_whenFilterExistsButNoSpecificProjection() {
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

        IndexFilterValue from = intValue(1, true);
        IndexFilterValue to = intValue(count / 2, true);
        IndexFilter filter = new IndexRangeFilter(from, true, to, true);

        MapScanMetadata scanMetadata = new MapScanMetadata(
                map.getName(),
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                Arrays.asList(QueryPath.KEY_PATH, valuePath("this.name"), valuePath("this.age")),
                Arrays.asList(QueryDataType.INT, VARCHAR, QueryDataType.INT),
                asList(
                        ColumnExpression.create(0, INT),
                        ColumnExpression.create(1, VARCHAR),
                        ColumnExpression.create(2, INT)
                ),
                new ConstantPredicateExpression(true)
        );

        MapIndexScanMetadata indexScanMetadata = new MapIndexScanMetadata(
                scanMetadata,
                indexConfig.getName(),
                1,
                filter,
                emptyList(),
                emptyList()
        );

        TestSupport
                .verifyProcessor(adaptSupplier(OnHeapMapIndexScanP.onHeapMapIndexScanP(indexScanMetadata)))
                .jetInstance(instance())
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .outputChecker(LENIENT_SAME_ITEMS_ANY_ORDER)
                .disableSnapshots()
                .disableProgressAssertion()
                .expectOutput(expected);
    }

    @Test
    public void test_whenFilterAndSpecificProjectionExists() {
        List<Object[]> expected = new ArrayList<>();
        for (int i = count; i > 0; i--) {
            map.put(i, new Person("value-" + i, i));
            if (i > count / 2) {
                expected.add(new Object[]{(count - i + 1), "value-" + (count - i + 1), (count - i + 1) * 5});
            }
        }

        IndexConfig indexConfig = new IndexConfig(IndexType.SORTED, "age");
        indexConfig.setName(randomName());
        map.addIndex(indexConfig);

        IndexFilterValue from = intValue(1, true);
        IndexFilterValue to = intValue(count / 2, true);
        IndexFilter filter = new IndexRangeFilter(from, true, to, true);

        MapScanMetadata scanMetadata = new MapScanMetadata(
                map.getName(),
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                Arrays.asList(QueryPath.KEY_PATH, valuePath("this.name"), valuePath("this.age")),
                Arrays.asList(QueryDataType.INT, VARCHAR, QueryDataType.INT),
                asList(
                        ColumnExpression.create(0, INT),
                        ColumnExpression.create(1, VARCHAR),
                        MultiplyFunction.create(
                                ColumnExpression.create(2, INT),
                                ConstantExpression.create(5, INT),
                                INT
                        )
                ),
                new ConstantPredicateExpression(true)
        );

        MapIndexScanMetadata indexScanMetadata = new MapIndexScanMetadata(
                scanMetadata,
                indexConfig.getName(),
                1,
                filter,
                emptyList(),
                emptyList()
        );

        TestSupport
                .verifyProcessor(adaptSupplier(OnHeapMapIndexScanP.onHeapMapIndexScanP(indexScanMetadata)))
                .jetInstance(instance())
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .outputChecker(LENIENT_SAME_ITEMS_ANY_ORDER)
                .disableSnapshots()
                .disableProgressAssertion()
                .expectOutput(expected);
    }

    // Real edge case when IN + RANGE filter didn't work together.
    @Test
    public void test_whenComplexFilterExistsWithoutSpecificProjection() {
        List<Object[]> expected = new ArrayList<>();
        for (int i = count; i > 0; i--) {
            map.put(i, new Person("value-" + i, i));
        }
        expected.add(new Object[]{2, "value-2", 2});
        expected.add(new Object[]{3, "value-3", 3});

        IndexConfig indexConfig = new IndexConfig(IndexType.SORTED, "age", "name");
        indexConfig.setName(randomName());
        map.addIndex(indexConfig);

        // SQL analogue : SELECT * FROM map WHERE (name = 'value-2' OR name = 'value-3') AND age = 2

        IndexFilter rhs = new IndexInFilter(
                new IndexEqualsFilter(new IndexFilterValue(singletonList(constant("value-2", VARCHAR)), singletonList(false))),
                new IndexEqualsFilter(new IndexFilterValue(singletonList(constant("value-3", VARCHAR)), singletonList(false)))
        );
        IndexFilter lhs = new IndexRangeFilter(
                intValue(1, false), true,
                intValue(4, false), true
        );

        IndexFilter filter = composeFilter(asList(lhs, rhs), IndexType.SORTED, 2);

        MapScanMetadata scanMetadata = new MapScanMetadata(
                map.getName(),
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                Arrays.asList(QueryPath.KEY_PATH, valuePath("this.name"), valuePath("this.age")),
                Arrays.asList(QueryDataType.INT, VARCHAR, QueryDataType.INT),
                asList(
                        ColumnExpression.create(0, INT),
                        ColumnExpression.create(1, VARCHAR),
                        ColumnExpression.create(2, INT)
                ),
                new ConstantPredicateExpression(true)
        );

        MapIndexScanMetadata indexScanMetadata = new MapIndexScanMetadata(
                scanMetadata,
                indexConfig.getName(),
                2,
                filter,
                emptyList(),
                emptyList()
        );

        TestSupport
                .verifyProcessor(adaptSupplier(OnHeapMapIndexScanP.onHeapMapIndexScanP(indexScanMetadata)))
                .jetInstance(instance())
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .outputChecker(LENIENT_SAME_ITEMS_ANY_ORDER)
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

    private static IndexFilterValue intValue(Integer value, boolean allowNull) {
        return new IndexFilterValue(
                Collections.singletonList(constant(value, QueryDataType.INT)),
                Collections.singletonList(allowNull)
        );
    }

    private static ConstantExpression constant(Object value, QueryDataType type) {
        return ConstantExpression.create(value, type);
    }

}
