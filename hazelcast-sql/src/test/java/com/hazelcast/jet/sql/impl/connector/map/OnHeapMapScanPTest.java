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

import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.ConstantPredicateExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.FunctionalPredicateExpression;
import com.hazelcast.sql.impl.expression.math.MultiplyFunction;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
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
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

@RunWith(Parameterized.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OnHeapMapScanPTest extends SimpleTestInClusterSupport {

    @Parameterized.Parameters(name = "count:{0}")
    public static Collection<Integer> parameters() {
        return asList(500, 25_000);
    }

    @Parameterized.Parameter()
    public int count;

    private IMap<Integer, String> map;

    @SuppressWarnings("unchecked")
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
    public void test_whenEmpty() {
        MapScanMetadata scanMetadata = new MapScanMetadata(
                map.getName(),
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                Arrays.asList(QueryPath.KEY_PATH, valuePath("this")),
                Arrays.asList(QueryDataType.INT, VARCHAR),
                Collections.emptyList(),
                new ConstantPredicateExpression(true)
        );

        TestSupport
                .verifyProcessor(adaptSupplier(OnHeapMapScanP.onHeapMapScanP(scanMetadata)))
                .jetInstance(instance())
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .disableSnapshots()
                .disableProgressAssertion()
                .expectOutput(emptyList());
    }

    @Test
    public void test_whenNoFilterAndNoSpecificProjection() {
        List<Object[]> expected = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            map.put(i, "value-" + i);
            expected.add(new Object[]{i, "value-" + i});
        }

        MapScanMetadata scanMetadata = new MapScanMetadata(
                map.getName(),
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                Arrays.asList(QueryPath.KEY_PATH, valuePath("this")),
                Arrays.asList(QueryDataType.INT, VARCHAR),
                asList(
                        ColumnExpression.create(0, INT),
                        ColumnExpression.create(1, VARCHAR)
                ),
                new ConstantPredicateExpression(true)
        );

        TestSupport
                .verifyProcessor(adaptSupplier(OnHeapMapScanP.onHeapMapScanP(scanMetadata)))
                .jetInstance(instance())
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .outputChecker(LENIENT_SAME_ITEMS_ANY_ORDER)
                .disableSnapshots()
                .disableProgressAssertion()
                .expectOutput(expected);
    }

    @Test
    public void test_whenFilterExistsAndNoProjection() {
        IMap<Integer, String> map = instance().getMap(randomMapName());
        List<Object[]> expected = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            map.put(i, "value-" + i);
            if (i % 2 == 0) {
                expected.add(new Object[]{i, "value-" + i});
            }
        }

        Expression<Boolean> filter = new FunctionalPredicateExpression(row -> {
            int value = row.get(0);
            return value % 2 == 0;
        });

        MapScanMetadata scanMetadata = new MapScanMetadata(
                map.getName(),
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                Arrays.asList(QueryPath.KEY_PATH, valuePath("this")),
                Arrays.asList(QueryDataType.INT, VARCHAR),
                asList(
                        ColumnExpression.create(0, INT),
                        ColumnExpression.create(1, VARCHAR)
                ),
                filter
        );

        TestSupport
                .verifyProcessor(adaptSupplier(OnHeapMapScanP.onHeapMapScanP(scanMetadata)))
                .jetInstance(instance())
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .outputChecker(LENIENT_SAME_ITEMS_ANY_ORDER)
                .disableSnapshots()
                .disableProgressAssertion()
                .expectOutput(expected);
    }

    @Test
    public void test_whenNoFilterButProjectionExists() {
        IMap<Integer, Person> objectMap = instance().getMap(randomMapName());

        List<Object[]> expected = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            objectMap.put(i, new Person("value-" + i, count - i));
            expected.add(new Object[]{i, "value-" + i, (count - i) * 5});
        }

        MapScanMetadata scanMetadata = new MapScanMetadata(
                objectMap.getName(),
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

        TestSupport
                .verifyProcessor(adaptSupplier(OnHeapMapScanP.onHeapMapScanP(scanMetadata)))
                .jetInstance(instance())
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .outputChecker(LENIENT_SAME_ITEMS_ANY_ORDER)
                .disableSnapshots()
                .disableProgressAssertion()
                .expectOutput(expected);
    }

    @Test
    public void test_filterMatchingOneRow() {
        for (int i = 0; i < count; i++) {
            map.put(i, "value-" + i);
        }
        map.put(-1, "value--1");
        List<Object[]> expected = singletonList(new Object[]{-1, "value--1"});

        MapScanMetadata scanMetadata = new MapScanMetadata(
                map.getName(),
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                Arrays.asList(QueryPath.KEY_PATH, valuePath("this")),
                Arrays.asList(QueryDataType.INT, VARCHAR),
                asList(
                        ColumnExpression.create(0, INT),
                        ColumnExpression.create(1, VARCHAR)
                ),
                new FunctionalPredicateExpression(row -> (int) row.get(0) == -1)
        );

        TestSupport
                .verifyProcessor(adaptSupplier(OnHeapMapScanP.onHeapMapScanP(scanMetadata)))
                .jetInstance(instance())
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .outputChecker(LENIENT_SAME_ITEMS_ANY_ORDER)
                .disableSnapshots()
                .disableProgressAssertion()
                .expectOutput(expected);
    }

    private static class Person implements DataSerializable {
        private String name;
        private int age;

        @SuppressWarnings("unused")
        Person() {
        }

        Person(String name, int age) {
            this.name = name;
            this.age = age;
        }

        public String getName() {
            return name;
        }

        public int getAge() {
            return age;
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
}
