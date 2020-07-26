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
import com.hazelcast.sql.impl.plan.node.MapIndexScanPlanNode;
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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings({"unchecked", "rawtypes", "unused"})
public abstract class SqlIndexAbstractTest extends SqlIndexTestSupport {

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
    public FieldDescriptor<?> f1;

    @Parameterized.Parameter(3)
    public FieldDescriptor<?> f2;

    protected final String mapName = "map" + MAP_NAME_GEN.incrementAndGet();

    private IMap<Integer, Value> map;
    private Class<? extends Value> valueClass;
    private int runIdGen;

    @Parameterized.Parameters(name = "indexType:{0}, composite:{1}, field1:{2}, field2:{3}")
    public static Collection<Object[]> parameters() {
        List<Object[]> res = new ArrayList<>();

        for (IndexType indexType : Arrays.asList(IndexType.SORTED, IndexType.HASH)) {
            for (boolean composite : Arrays.asList(true, false)) {
                for (FieldDescriptor<?> firstFieldDescriptor : FIELD_DESCRIPTORS) {
                    for (FieldDescriptor<?> secondFieldDescriptor : FIELD_DESCRIPTORS) {
                        res.add(new Object[] { indexType, composite, firstFieldDescriptor, secondFieldDescriptor });
                    }
                }
            }
        }

        return res;
    }

    @Before
    public void before() throws Exception {
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

        valueClass = getValueClass(f1, f2);

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

    private void fill(IMap map) throws Exception {
        // Create an object with non-null fields to initialize converters
        Value initialValue = valueClass.newInstance();
        initialValue.setField1(f1.valueFrom());
        initialValue.setField2(f2.valueFrom());

        for (HazelcastInstance member : members) {
            int memberKey = getLocalKey(member, value -> value);

            map.put(memberKey, initialValue);
            map.remove(memberKey);
        }

        // Fill with values
        int key = 0;

        Map localMap = new HashMap();

        for (Object firstField : f1.values()) {
            for (Object secondField : f2.values()) {
                Value value = valueClass.newInstance();

                value.setField1(firstField);
                value.setField2(secondField);

                // Put the same value twice intentionally to test index key with multiple values
                for (int i = 0; i < 2 * getMemberCount(); i++) {
                    int currentKey = key++;

                    value.key = currentKey;
                    localMap.put(currentKey, value);
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
            eq_1(f1.valueFrom())
        );

        check0(
            query(f1.toLiteral(f1.valueFrom()) + "=field1"),
            c_sortedOrHashNotComposite(),
            eq_1(f1.valueFrom())
        );

        // WHERE f1=?
        check0(
            query("field1=?", p_1(f1.valueFrom())),
            c_sortedOrHashNotComposite(),
            eq_1(f1.valueFrom())
        );

        check0(
            query("?=field1", p_1(f1.valueFrom())),
            c_sortedOrHashNotComposite(),
            eq_1(f1.valueFrom())
        );

        // WHERE f1 IS NULL
        check0(
            query("field1 IS NULL"),
            c_sortedOrHashNotComposite(),
            null_1()
        );

        // WHERE f1>literal
        check0(
            query("field1>" + f1.toLiteral(f1.valueFrom())),
            c_sortedOrHashNotCompositeAndBooleanComponent(),
            gt_1(f1.valueFrom())
        );

        check0(
            query(f1.toLiteral(f1.valueFrom()) + "<field1"),
            c_sortedOrHashNotCompositeAndBooleanComponent(),
            gt_1(f1.valueFrom())
        );

        // WHERE f1>=literal
        check0(
            query("field1>=" + f1.toLiteral(f1.valueFrom())),
            c_sorted(),
            gte_1(f1.valueFrom())
        );

        check0(
            query(f1.toLiteral(f1.valueFrom()) + "<=field1"),
            c_sorted(),
            gte_1(f1.valueFrom())
        );

        // WHERE f1<literal
        check0(
            query("field1<" + f1.toLiteral(f1.valueFrom())),
            c_sorted(),
            lt_1(f1.valueFrom())
        );

        check0(
            query(f1.toLiteral(f1.valueFrom()) + ">field1"),
            c_sorted(),
            lt_1(f1.valueFrom())
        );

        // WHERE f1<=literal
        check0(
            query("field1<=" + f1.toLiteral(f1.valueFrom())),
            c_sortedOrHashNotCompositeAndBooleanComponent(),
            lte_1(f1.valueFrom())
        );

        check0(
            query(f1.toLiteral(f1.valueFrom()) + ">=field1"),
            c_sortedOrHashNotCompositeAndBooleanComponent(),
            lte_1(f1.valueFrom())
        );

        // TODO: Range open, param
        // TODO: Range both, literal/literal
        // TODO: Range both, param/param

        // TODO: IN (simple, composite)

        // Special cases for boolean field
        if (f1 instanceof BooleanFieldDescriptor) {
            // WHERE f1
            check0(
                query("field1"),
                c_sortedOrHashNotComposite(),
                eq_1(true)
            );

            // WHERE f1 IS TRUE
            check0(
                query("field1 IS TRUE"),
                c_sortedOrHashNotComposite(),
                eq_1(true)
            );

            // WHERE f1 IS FALSE
            check0(
                query("field1 IS FALSE"),
                c_sortedOrHashNotComposite(),
                eq_1(false)
            );

            // WHERE f1 IS NOT TRUE
            check0(
                query("field1 IS NOT TRUE"),
                c_sortedOrHashNotComposite(),
                or(eq_1(false), null_1())
            );

            // WHERE f1 IS NOT FALSE
            check0(
                query("field1 IS NOT FALSE"),
                c_sortedOrHashNotComposite(),
                or(eq_1(true), null_1())
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
        return c_sorted() || (!composite && f1 instanceof BooleanFieldDescriptor);
    }

    private void check(Query query, boolean expectedUseIndex, Predicate<Value> expectedKeysPredicate) {
        // Prepare two additional queries with an additional AND/OR predicate
        String condition = "key / 2 = 0";
        Query queryWithAnd = addConditionToQuery(query, condition, true);
        Query queryWithOr = addConditionToQuery(query, condition, false);

        Predicate<Value> predicate = value -> value.key / 2 == 0;
        Predicate<Value> expectedKeysPredicateWithAnd = and(expectedKeysPredicate, predicate);
        Predicate<Value> expectedKeysPredicateWithOr = or(expectedKeysPredicate, predicate);

        // Run the original query
        check0(query, expectedUseIndex, expectedKeysPredicate);

        // Run query with AND, the same index should be used
        check0(queryWithAnd, expectedUseIndex, expectedKeysPredicateWithAnd);

        // Run query with OR, no index should be used
        check0(queryWithOr, false, expectedKeysPredicateWithOr);
    }

    private void check0(Query query, boolean expectedUseIndex, Predicate<Value> expectedKeysPredicate) {
        System.out.println(">>> SQL: " + query.sql);

        for (List<Object> params0 : generateParameters(query.parameters)) {
            check0(query.sql, params0, expectedUseIndex, expectedKeysPredicate);
        }
    }

    private void check0(String sql, List<Object> params, boolean expectedUseIndex, Predicate<Value> expectedKeysPredicate) {
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
            MapIndexScanPlanNode indexNode = findIndexNode(result);

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

    private Set<Integer> expectedMapKeys(Predicate<Value> predicate) {
        Set<Integer> keys = new HashSet<>();

        for (Map.Entry<Integer, Value> entry : map.entrySet()) {
            Integer key = entry.getKey();
            Value value = entry.getValue();

            if (predicate.test(value)) {
                keys.add(key);
            }
        }

        return keys;
    }

    private Set<Integer> expectedSqlKeys(Predicate<Value> predicate) {
        String sql = "SELECT __key, this FROM " + mapName;

        SqlQuery query = new SqlQuery(sql);

        Set<Integer> keys = new HashSet<>();

        try (SqlResult result = member.getSql().query(query)) {
            for (SqlRow row : result) {
                Integer key = row.getObject(0);
                Value value = row.getObject(1);

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
        FieldDescriptor f = parameter.first ? f1 : f2;

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
