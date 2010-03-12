/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.query;

import com.hazelcast.core.MapEntry;
import com.hazelcast.impl.Record;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

public class MapIndexServiceTest extends TestUtil {
    @Test
    public void testIndex() throws Exception {
        MapIndexService mapIndexService = new MapIndexService();
        assertFalse(mapIndexService.hasIndexedAttributes());
        Expression nameExpression = Predicates.get("name");
        Expression ageExpression = Predicates.get("age");
        Expression salaryExpression = Predicates.get("salary");
        mapIndexService.addIndex(nameExpression, false, 0);
        mapIndexService.addIndex(ageExpression, true, 1);
        mapIndexService.addIndex(salaryExpression, true, 2);
        Map<Expression, MapIndex> indexes = mapIndexService.getIndexes();
        assertTrue(indexes.containsKey(nameExpression));
        assertTrue(indexes.containsKey(ageExpression));
        assertEquals(3, indexes.size());
        assertTrue(mapIndexService.hasIndexedAttributes());
        for (int i = 0; i < 100; i++) {
            Employee employee = new Employee("Name" + i, i % 80, true, 100 + i);
            Record record = newRecord(i, "key" + i, employee);
            record.setIndexes(mapIndexService.getIndexValues(employee), mapIndexService.getIndexTypes());
            mapIndexService.index(record);
        }
        QueryContext queryContext = new QueryContext("default", new SqlPredicate ("age >=10 and age <=20"));
        Set<MapEntry> results = mapIndexService.doQuery(queryContext);
        System.out.println("result size " + results.size());
        for (MapEntry entry : results) {
            System.out.println(entry.getValue());
        }
    }

    class ExpressionImpl<T> implements Expression<T> {
        final String name;
        final T value;

        ExpressionImpl(String name, T value) {
            this.value = value;
            this.name = name;
        }

        public T getValue(Object obj) {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ExpressionImpl that = (ExpressionImpl) o;
            if (name != null ? !name.equals(that.name) : that.name != null) return false;
            return true;
        }

        @Override
        public int hashCode() {
            return name != null ? name.hashCode() : 0;
        }
    }
}
