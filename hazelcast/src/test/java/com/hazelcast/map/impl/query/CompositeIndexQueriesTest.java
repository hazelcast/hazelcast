/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.query;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.QueryException;
import com.hazelcast.query.impl.Extractable;
import com.hazelcast.query.impl.IndexUtils;
import com.hazelcast.query.impl.IndexRegistry;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.predicates.IndexAwarePredicate;
import com.hazelcast.query.impl.predicates.SqlPredicate;
import com.hazelcast.query.impl.predicates.VisitablePredicate;
import com.hazelcast.query.impl.predicates.Visitor;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CompositeIndexQueriesTest extends HazelcastTestSupport {

    private IMap<Integer, Person> map;

    private List<String> indexes = new ArrayList<>();

    @Parameterized.Parameters(name = "format:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{{InMemoryFormat.OBJECT}, {InMemoryFormat.BINARY}});
    }

    @Parameterized.Parameter
    public InMemoryFormat inMemoryFormat;

    @Before
    public void before() {
        Config config = getConfig();
        config.getMapConfig("map").setInMemoryFormat(inMemoryFormat);
        config.setProperty(QueryEngineImpl.DISABLE_MIGRATION_FALLBACK.getName(), "true");
        map = createHazelcastInstance(config).getMap("map");

        IndexConfig indexConfig1 = IndexUtils.createTestIndexConfig(IndexType.HASH, "name", "age");
        IndexConfig indexConfig2 = IndexUtils.createTestIndexConfig(IndexType.SORTED, "__key", "age");
        IndexConfig indexConfig3 = IndexUtils.createTestIndexConfig(IndexType.SORTED, "height", "__key");

        map.addIndex(indexConfig1);
        map.addIndex(indexConfig2);
        map.addIndex(indexConfig3);

        indexes.add(indexConfig1.getName());
        indexes.add(indexConfig2.getName());
        indexes.add(indexConfig3.getName());

        map.put(-2, new Person(null));
        map.put(-1, new Person(null));
        for (int i = 0; i < 100; ++i) {
            map.put(i, new Person(i));
        }
        map.put(100, new Person(null));
        map.put(101, new Person(null));
    }

    @Test
    public void testCompositeQueries() {
        check(null, 0, 0, 0, 0);

        check("name = '010' and age = 110", 1, 1, 0, 0);
        check("this.age = 110 and name = '010'", 1, 2, 0, 0);

        check("__key = 10 and age = 110", 1, 2, 1, 0);
        check("age = 110 and __key = 10", 1, 2, 2, 0);

        check("__key = '10' and age >= 110", 1, 2, 3, 0);
        check("age >= 110 and __key = 10", 1, 2, 4, 0);

        check("name = '010' and age >= 110 and __key = 10", 1, 2, 5, 0);
        check("name <= '010' and age >= 110 and __key = 10", 1, 2, 6, 0);
        check("age >= 110 and __key = 10 and unindexedAge <= 110", 1, 2, 7, 0);

        map.put(101, new Person(10));
        check("name = '010' and age = 110", 2, 3, 7, 0);
        map.removeAll(Predicates.sql("name = '010' and age = 110"));
        check("name = '010' and age = 110", 0, 5, 7, 0);
    }

    @Test
    public void testFirstComponentQuerying() {
        check(null, 0, 0, 0, 0);

        check("name < '050'", 50, 0, 0, 0);
        check("__key < 10", 12, 0, 1, 0);
        check("__key < 50 and __key >= 10", 40, 0, 2, 0);
        check("__key between 10 and 49", 40, 0, 3, 0);

        check("__key = -1", 1, 0, 4, 0);
        check("__key >= 100", 2, 0, 5, 0);
        check("__key > 99", 2, 0, 6, 0);
        check("__key > 100", 1, 0, 7, 0);
        check("__key < 0", 2, 0, 8, 0);
        check("__key <= -1", 2, 0, 9, 0);
        check("__key < -1", 1, 0, 10, 0);
        check("__key > 101", 0, 0, 11, 0);
        check("__key > 100", 1, 0, 12, 0);
        check("__key >= 101", 1, 0, 13, 0);
        check("__key <= 0", 3, 0, 14, 0);

        check("__key >= 50 and height >= 50", 50, 0, 15, 1);

        check("__key in (-1)", 1, 0, 16, 1);
        check("__key in (-2, 50, 101)", 3, 0, 17, 1);
        check("__key in (-2, 50, -2)", 2, 0, 18, 1);
        check("__key in (50, 50)", 1, 0, 19, 1);

        map.put(101, new Person(102));
        check("__key >= 50 and height >= 50", 51, 0, 20, 2);
        map.removeAll(Predicates.equal("__key", 101));
        check("__key >= 50 and height >= 50", 50, 0, 22, 3);
    }

    @Test
    public void testNonCompositeQueries() {
        check(null, 0, 0, 0, 0);

        check("name = '010' and age >= 110", 1, 0, 0, 0);
        check("age >= 110 and name = '010'", 1, 0, 0, 0);

        check("name > '009' and age > 109", 90, 0, 0, 0);
        check("age >= 109 and this.name > '009'", 90, 0, 0, 0);

        check("age > 99", 100, 0, 0, 0);
        check("name = '050'", 1, 0, 0, 0);
        check("name = null", 4, 0, 0, 0);

        check("height != null", 100, 0, 0, 0);
    }

    @Test
    public void testNulls() {
        check(null, 0, 0, 0, 0);

        check("this.name = null and age = null", 4, 1, 0, 0);
        check("__key = 101 and age = null", 1, 1, 1, 0);

        check("age < 1000", 100, 1, 1, 0);

        check("__key = 102 and age > -1000", 0, 1, 2, 0);
        check("__key = 101 and age < 1000", 0, 1, 3, 0);

        check("height = null and __key > 99", 2, 1, 3, 1);
        check("height = null and __key <= 101", 4, 1, 3, 2);
        check("height = null", 4, 1, 3, 3);

        check("height = null and __key >= 100 and __key <= 101", 2, 1, 3, 4);
        check("height = null and __key >= 100 and __key < 102", 2, 1, 3, 5);
        check("height = null and __key > 99 and __key < 102", 2, 1, 3, 6);
        check("height = null and __key > 99 and __key <= 101", 2, 1, 3, 7);

        map.put(50, new Person(null));
        check("this.name = null and age = null", 5, 2, 3, 7);
        map.removeAll(Predicates.sql("this.name = null and age = null"));
        check("this.name = null and age = null", 0, 4, 3, 7);
    }

    protected Config getConfig() {
        return smallInstanceConfig();
    }

    private void check(String sql, int expectedSize, int... queryCounts) {
        if (sql != null) {
            SqlPredicate sqlPredicate = (SqlPredicate) Predicates.sql(sql);
            Set<Map.Entry<Integer, Person>> result = map.entrySet(sqlPredicate);
            assertEquals(expectedSize, result.size());

            // here we are checking the original unoptimized predicate
            for (Map.Entry<Integer, Person> entry : result) {
                assertTrue(sqlPredicate.apply(new ExtractableAdapter(entry)));
            }

            // The goal here is to check fallback predicates generated by
            // CompositeEqualPredicate and CompositeRangePredicate.
            Set<Map.Entry<Integer, Person>> noIndexResult = map.entrySet(new NoIndexPredicate(sqlPredicate.getPredicate()));
            assertEquals(result, noIndexResult);
        }

        assert indexes.size() == queryCounts.length;
        for (int i = 0; i < queryCounts.length; ++i) {
            assertEquals(queryCounts[i], map.getLocalMapStats().getIndexStats().get(indexes.get(i)).getQueryCount());
        }
    }

    public static class Person implements Serializable {

        public final String name;
        public final Long age;
        public final Integer height;
        public final Long unindexedAge;

        public Person(Integer value) {
            this.name = value == null ? null : String.format("%03d", value);
            this.age = value == null ? null : (long) value + 100;
            this.height = value;
            this.unindexedAge = age;
        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + (age != null ? age.hashCode() : 0);
            result = 31 * result + (height != null ? height.hashCode() : 0);
            result = 31 * result + (unindexedAge != null ? unindexedAge.hashCode() : 0);
            return result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Person person = (Person) o;

            if (name != null ? !name.equals(person.name) : person.name != null) {
                return false;
            }
            if (age != null ? !age.equals(person.age) : person.age != null) {
                return false;
            }
            if (unindexedAge != null ? !unindexedAge.equals(person.unindexedAge) : person.unindexedAge != null) {
                return false;
            }
            return height != null ? height.equals(person.height) : person.height == null;
        }

    }

    private static class ExtractableAdapter implements Map.Entry<Integer, Person>, Extractable {

        private final Integer key;
        private final Person value;

        ExtractableAdapter(Map.Entry<Integer, Person> entry) {
            this.key = entry.getKey();
            this.value = entry.getValue();
        }

        @Override
        public Integer getKey() {
            return key;
        }

        @Override
        public Person getValue() {
            return value;
        }

        @Override
        public Person setValue(Person value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object getAttributeValue(String attributeName) throws QueryException {
            if (attributeName.equals("name")) {
                return value.name;
            }
            if (attributeName.equals("age")) {
                return value.age;
            }
            if (attributeName.equals("__key")) {
                return key;
            }
            if (attributeName.equals("height")) {
                return value.height;
            }
            if (attributeName.equals("unindexedAge")) {
                return value.unindexedAge;
            }

            throw new QueryException();
        }

    }

    private static class NoIndexPredicate implements IndexAwarePredicate, VisitablePredicate {

        private volatile Predicate delegate;

        NoIndexPredicate(Predicate delegate) {
            this.delegate = delegate;
        }

        @Override
        public Predicate accept(Visitor visitor, IndexRegistry indexes) {
            Predicate delegate = this.delegate;
            if (delegate instanceof VisitablePredicate predicate) {
                this.delegate = predicate.accept(visitor, indexes);
            }
            return this;
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean apply(Map.Entry mapEntry) {
            return delegate.apply(mapEntry);
        }

        @Override
        public Set<QueryableEntry> filter(QueryContext queryContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isIndexed(QueryContext queryContext) {
            return false;
        }

    }

}
