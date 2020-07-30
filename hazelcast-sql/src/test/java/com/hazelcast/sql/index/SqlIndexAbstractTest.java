/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlQuery;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.plan.node.MapIndexScanPlanNode;
import com.hazelcast.sql.support.expressions.ExpressionBiValue;
import com.hazelcast.sql.support.expressions.ExpressionType;
import com.hazelcast.sql.support.expressions.ExpressionTypes;
import com.hazelcast.sql.support.expressions.ExpressionValue;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static com.hazelcast.sql.support.expressions.ExpressionPredicates.and;
import static com.hazelcast.sql.support.expressions.ExpressionPredicates.eq;
import static com.hazelcast.sql.support.expressions.ExpressionPredicates.gt;
import static com.hazelcast.sql.support.expressions.ExpressionPredicates.gte;
import static com.hazelcast.sql.support.expressions.ExpressionPredicates.lt;
import static com.hazelcast.sql.support.expressions.ExpressionPredicates.lte;
import static com.hazelcast.sql.support.expressions.ExpressionPredicates.isNull;
import static com.hazelcast.sql.support.expressions.ExpressionPredicates.or;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings({"unchecked", "rawtypes", "unused"})
public abstract class SqlIndexAbstractTest extends SqlTestSupport {

    private static final AtomicInteger MAP_NAME_GEN = new AtomicInteger();
    private static final String INDEX_NAME = "index";
    private static final TestHazelcastInstanceFactory FACTORY = new TestHazelcastInstanceFactory(2);

    private static List<HazelcastInstance> members;
    private static HazelcastInstance member;

    @Parameterized.Parameter
    public IndexType indexType;

    @Parameterized.Parameter(1)
    public boolean composite;

    @Parameterized.Parameter(2)
    public ExpressionType<?> f1;

    @Parameterized.Parameter(3)
    public ExpressionType<?> f2;

    protected final String mapName = "map" + MAP_NAME_GEN.incrementAndGet();

    private IMap<Integer, ExpressionBiValue> map;
    private Class<? extends ExpressionBiValue> valueClass;
    private int runIdGen;

    @Parameterized.Parameters(name = "indexType:{0}, composite:{1}, field1:{2}, field2:{3}")
    public static Collection<Object[]> parameters() {
        List<Object[]> res = new ArrayList<>();

        for (IndexType indexType : Arrays.asList(IndexType.SORTED, IndexType.HASH)) {
            for (boolean composite : Arrays.asList(true, false)) {
                for (ExpressionType<?> firstType : ExpressionTypes.allTypes()) {
                    for (ExpressionType<?> secondType : ExpressionTypes.allTypes()) {
                        res.add(new Object[] { indexType, composite, firstType, secondType });
                    }
                }
            }
        }

        return res;
    }

    @Before
    public void before() {
        // Start members if needed
        if (members == null) {
            members = new ArrayList<>();

            assertTrue(getMemberCount() > 0);

            for (int i = 0; i < getMemberCount(); i++) {
                HazelcastInstance newMember = FACTORY.newHazelcastInstance(getConfig());

                members.add(newMember);

                if (i == 0) {
                    member = newMember;
                }
            }
        }

        valueClass = ExpressionBiValue.createBiClass(f1, f2);

        MapConfig mapConfig = getMapConfig();

        member.getConfig().addMapConfig(mapConfig);

        map = member.getMap(mapName);

        fill(map);
    }

    @After
    public void after() {
        member.getMap(mapName).destroy();
    }

    @AfterClass
    public static void afterClass() {
        members = null;
        member = null;

        FACTORY.shutdownAll();
    }

    private void fill(IMap map) {
        // Create an object with non-null fields to initialize converters
        for (HazelcastInstance member : members) {
            int key = getLocalKey(member, value -> value);
            ExpressionBiValue value = ExpressionBiValue.createBiValue(valueClass, key, f1.valueFrom(), f2.valueFrom());

            map.put(key, value);
            map.remove(key);
        }

        // Fill with values
        int keyCounter = 0;

        Map localMap = new HashMap();

        for (Object firstField : f1.values()) {
            for (Object secondField : f2.values()) {
                // Put the same value twice intentionally to test index key with multiple values
                for (int i = 0; i < 2 * getMemberCount(); i++) {
                    int key = keyCounter++;
                    ExpressionBiValue value = ExpressionBiValue.createBiValue(valueClass, key, firstField, secondField);

                    localMap.put(key, value);
                }
            }
        }

        map.putAll(localMap);
    }

    protected IndexConfig getIndexConfig() {
        IndexConfig config = new IndexConfig().setName(INDEX_NAME).setType(indexType);

        config.addAttribute("field1");

        if (composite) {
            config.addAttribute("field2");
        }

        return config;
    }

    @Test public void test() {
        checkFirstColumn();
        checkSecondColumn();
        checkBothColumns();
    }

    private void checkFirstColumn() {
        // WHERE f1=literal
        check0(
            query("field1=" + f1.toLiteral(f1.valueFrom())),
            c_sortedOrHashNotComposite(),
            eq(f1.valueFrom())
        );

        check0(
            query(f1.toLiteral(f1.valueFrom()) + "=field1"),
            c_sortedOrHashNotComposite(),
            eq(f1.valueFrom())
        );

        // WHERE f1=?
        check0(
            query("field1=?", p_1(f1.valueFrom())),
            c_sortedOrHashNotComposite(),
            eq(f1.valueFrom())
        );

        check0(
            query("?=field1", p_1(f1.valueFrom())),
            c_sortedOrHashNotComposite(),
            eq(f1.valueFrom())
        );

        // WHERE f1 IS NULL
        check0(
            query("field1 IS NULL"),
            c_sortedOrHashNotComposite(),
            isNull()
        );

        // WHERE f1>literal
        check0(
            query("field1>" + f1.toLiteral(f1.valueFrom())),
            c_sortedOrHashNotCompositeAndBooleanComponent(),
            gt(f1.valueFrom())
        );

        check0(
            query(f1.toLiteral(f1.valueFrom()) + "<field1"),
            c_sortedOrHashNotCompositeAndBooleanComponent(),
            gt(f1.valueFrom())
        );

        // WHERE f1>=literal
        check0(
            query("field1>=" + f1.toLiteral(f1.valueFrom())),
            c_sorted(),
            gte(f1.valueFrom())
        );

        check0(
            query(f1.toLiteral(f1.valueFrom()) + "<=field1"),
            c_sorted(),
            gte(f1.valueFrom())
        );

        // WHERE f1<literal
        check0(
            query("field1<" + f1.toLiteral(f1.valueFrom())),
            c_sorted(),
            lt(f1.valueFrom())
        );

        check0(
            query(f1.toLiteral(f1.valueFrom()) + ">field1"),
            c_sorted(),
            lt(f1.valueFrom())
        );

        // WHERE f1<=literal
        check0(
            query("field1<=" + f1.toLiteral(f1.valueFrom())),
            c_sortedOrHashNotCompositeAndBooleanComponent(),
            lte(f1.valueFrom())
        );

        check0(
            query(f1.toLiteral(f1.valueFrom()) + ">=field1"),
            c_sortedOrHashNotCompositeAndBooleanComponent(),
            lte(f1.valueFrom())
        );

        // TODO: Range open, param
        // TODO: Range both, literal/literal
        // TODO: Range both, param/param

        // TODO: IN (simple, composite)

        // Special cases for boolean field
        if (f1 instanceof ExpressionType.BooleanType) {
            // WHERE f1
            check0(
                query("field1"),
                c_sortedOrHashNotComposite(),
                eq(true)
            );

            // WHERE f1 IS TRUE
            check0(
                query("field1 IS TRUE"),
                c_sortedOrHashNotComposite(),
                eq(true)
            );

            // WHERE f1 IS FALSE
            check0(
                query("field1 IS FALSE"),
                c_sortedOrHashNotComposite(),
                eq(false)
            );

            // WHERE f1 IS NOT TRUE
            check0(
                query("field1 IS NOT TRUE"),
                c_sortedOrHashNotComposite(),
                or(eq(false), isNull())
            );

            // WHERE f1 IS NOT FALSE
            check0(
                query("field1 IS NOT FALSE"),
                c_sortedOrHashNotComposite(),
                or(eq(true), isNull())
            );
        }
    }

    private void checkSecondColumn() {
        // TODO
    }

    private void checkBothColumns() {
        // TODO
    }

    private boolean c_sorted() {
        return indexType == IndexType.SORTED;
    }

    private boolean c_sortedOrHashNotComposite() {
        return c_sorted() || !composite;
    }

    private boolean c_sortedOrHashNotCompositeAndBooleanComponent() {
        return c_sorted() || (!composite && f1 instanceof ExpressionType.BooleanType);
    }

    // TODO: check0 is mistakenly used here!
    private void check(Query query, boolean expectedUseIndex, Predicate<ExpressionValue> expectedKeysPredicate) {
        // Prepare two additional queries with an additional AND/OR predicate
        String condition = "key / 2 = 0";
        Query queryWithAnd = addConditionToQuery(query, condition, true);
        Query queryWithOr = addConditionToQuery(query, condition, false);

        Predicate<ExpressionValue> predicate = value -> value.key / 2 == 0;
        Predicate<ExpressionValue> expectedKeysPredicateWithAnd = and(expectedKeysPredicate, predicate);
        Predicate<ExpressionValue> expectedKeysPredicateWithOr = or(expectedKeysPredicate, predicate);

        // Run the original query
        check0(query, expectedUseIndex, expectedKeysPredicate);

        // Run query with AND, the same index should be used
        check0(queryWithAnd, expectedUseIndex, expectedKeysPredicateWithAnd);

        // Run query with OR, no index should be used
        check0(queryWithOr, false, expectedKeysPredicateWithOr);
    }

    private void check0(Query query, boolean expectedUseIndex, Predicate<ExpressionValue> expectedKeysPredicate) {
        System.out.println(">>> SQL: " + query.sql);

        for (List<Object> params0 : generateParameters(query.parameters)) {
            check0(query.sql, params0, expectedUseIndex, expectedKeysPredicate);
        }
    }

    private void check0(
        String sql,
        List<Object> params,
        boolean expectedUseIndex,
        Predicate<ExpressionValue> expectedKeysPredicate
    ) {
        int runId = runIdGen++;

        Set<Integer> sqlKeys = sqlKeys(expectedUseIndex, sql, params);
        Set<Integer> expectedSqlKeys = expectedSqlKeys(expectedKeysPredicate);
        Set<Integer> expectedMapKeys = expectedMapKeys(expectedKeysPredicate);

        if (!expectedSqlKeys.equals(expectedMapKeys)) {
            failOnDifference(
                runId,
                sql,
                params,
                expectedSqlKeys,
                expectedMapKeys,
                "expected SQL keys differ from expected map keys",
                "expected SQl keys",
                "expected map keys"
            );
        }

        if (!sqlKeys.equals(expectedMapKeys)) {
            failOnDifference(
                runId,
                sql,
                params,
                sqlKeys,
                expectedMapKeys,
                "actual SQL keys differ from expected map keys",
                "actual SQL keys",
                "expected map keys"
            );
        }
    }

    @SuppressWarnings("StringConcatenationInsideStringBufferAppend")
    private void failOnDifference(
        int runId,
        String sql,
        List<Object> params,
        Set<Integer> first,
        Set<Integer> second,
        String mainMessage,
        String firstCaption,
        String secondCaption
    ) {
        List<ParameterToString> params0 = new ArrayList<>(params.size());

        for (Object param : params) {
            params0.add(new ParameterToString(param));
        }

        Set<Integer> firstOnly = new TreeSet<>(first);
        Set<Integer> secondOnly = new TreeSet<>(second);

        firstOnly.removeAll(second);
        secondOnly.removeAll(first);

        assertTrue(!firstOnly.isEmpty() || !secondOnly.isEmpty());

        StringBuilder message = new StringBuilder();

        message.append("\nRun " + runId + " failed: " + mainMessage + "\n\n");
        message.append("SQL: " + sql + "\n");
        message.append("Parameters: " + params0 + "\n\n");

        if (!firstOnly.isEmpty()) {
            message.append("\t" + firstCaption + ":\n");

            for (Integer key : firstOnly) {
                message.append("\t\t" + key + " -> " + map.get(key) + "\n");
            }
        }

        if (!secondOnly.isEmpty()) {
            message.append("\t" + secondCaption + ":\n");

            for (Integer key : secondOnly) {
                message.append("\t\t" + key + " -> " + map.get(key) + "\n");
            }
        }

        fail(message.toString());
    }

    private static Query addConditionToQuery(Query query, String condition, boolean conjunction) {
        String sql = query.sql;

        if (sql.contains("WHERE")) {
            sql = sql + " " + (conjunction ? "AND" : "OR") + " " + condition;
        } else {
            sql = sql + " WHERE " + condition;
        }

        return new Query(sql, query.parameters);
    }

    private String sql(String condition) {
        return "SELECT __key FROM " + mapName + " WHERE " + condition;
    }

    private Set<Integer> sqlKeys(boolean withIndex, String sql, List<Object> params) {
        SqlQuery query = new SqlQuery(sql);

        if (!params.isEmpty()) {
            query.setParameters(params);
        }

        Set<Integer> keys = new HashSet<>();

        try (SqlResult result = member.getSql().query(query)) {
            MapIndexScanPlanNode indexNode = findFirstIndexNode(result);

            if (withIndex) {
                assertNotNull(indexNode);
            } else {
                if (isHd()) {
                    assertNotNull(indexNode);
                    assertNull(indexNode.getIndexFilter());
                } else {
                    assertNull(indexNode);
                }
            }

            for (SqlRow row : result) {
                keys.add(row.getObject(0));
            }
        }

        return keys;
    }

    private Set<Integer> expectedMapKeys(Predicate<ExpressionValue> predicate) {
        Set<Integer> keys = new HashSet<>();

        for (Map.Entry<Integer, ExpressionBiValue> entry : map.entrySet()) {
            Integer key = entry.getKey();
            ExpressionBiValue value = entry.getValue();

            if (predicate.test(value)) {
                keys.add(key);
            }
        }

        return keys;
    }

    private Set<Integer> expectedSqlKeys(Predicate<ExpressionValue> predicate) {
        String sql = "SELECT __key, this FROM " + mapName;

        SqlQuery query = new SqlQuery(sql);

        Set<Integer> keys = new HashSet<>();

        try (SqlResult result = member.getSql().query(query)) {
            for (SqlRow row : result) {
                Integer key = row.getObject(0);
                ExpressionBiValue value = row.getObject(1);

                if (predicate.test(value)) {
                    keys.add(key);
                }
            }
        }

        return keys;
    }

    protected abstract int getMemberCount();

    protected abstract boolean isHd();

    private Parameter p_1(Object parameter) {
        return new Parameter(parameter, true);
    }

    private Parameter p_2(Object parameter) {
        return new Parameter(parameter, false);
    }

    private List<List<Object>> generateParameters(Parameter... parameters) {
        if (parameters == null || parameters.length == 0) {
            return Collections.singletonList(Collections.emptyList());
        }

        List<List<Object>> res = new ArrayList<>();

        generateParameters0(parameters, 0, res, Collections.emptyList());

        return res;
    }

    private void generateParameters0(Parameter[] parameters, int index, List<List<Object>> res, List<Object> current) {
        List<Object> parameterVariations = parameterVariations(parameters[index]);

        for (Object parameterVariation : parameterVariations) {
            List<Object> current0 = new ArrayList<>(current);

            current0.add(parameterVariation);

            if (index == parameters.length - 1) {
                res.add(current0);
            } else {
                generateParameters0(parameters, index + 1, res, current0);
            }
        }
    }

    private List<Object> parameterVariations(Parameter parameter) {
        ExpressionType f = parameter.first ? f1 : f2;

        return new ArrayList(f.parameterVariations(parameter.parameter));
    }

    private Query query(String condition, Parameter... parameters) {
        return new Query(sql(condition), parameters);
    }

    protected MapConfig getMapConfig() {
        return new MapConfig().setName(mapName).setBackupCount(0).addIndexConfig(getIndexConfig());
    }

    private static class Query {

        private final String sql;
        private final Parameter[] parameters;

        private Query(String sql, Parameter[] parameters) {
            this.sql = sql;
            this.parameters = parameters;
        }
    }

    private static class Parameter {

        private final Object parameter;
        private final boolean first;

        private Parameter(Object parameter, boolean first) {
            this.parameter = parameter;
            this.first = first;
        }
    }

    private static class ParameterToString {
        private final Object parameter;

        private ParameterToString(Object parameter) {
            this.parameter = parameter;
        }

        @Override
        public String toString() {
            if (parameter == null) {
                return "null";
            } else {
                return parameter + ":" + parameter.getClass().getSimpleName();
            }
        }
    }
}
