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

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.MapEntry;
import com.hazelcast.impl.Record;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MapIndexServiceTest extends TestUtil {

    @Test
    public void testIndex() throws Exception {
        MapIndexService mapIndexService = new MapIndexService();
        Map<Expression, Index> indexes = mapIndexService.getIndexes();
        assertFalse(mapIndexService.hasIndexedAttributes());
        Expression nameExpression = Predicates.get("name");
        Expression ageExpression = Predicates.get("age");
        Expression salaryExpression = Predicates.get("salary");
        mapIndexService.addIndex(nameExpression, false, 0);
        mapIndexService.addIndex(ageExpression, true, 1);
        mapIndexService.addIndex(salaryExpression, true, 2);
        assertTrue(indexes.containsKey(nameExpression));
        assertTrue(indexes.containsKey(ageExpression));
        assertTrue(indexes.containsKey(salaryExpression));
//        assertEquals(2, indexes.size());
        assertTrue(mapIndexService.hasIndexedAttributes());
        for (int i = 0; i < 200000; i++) {
            Employee employee = new Employee(i + "Name", i % 80, (i % 2 == 0), 100 + (i % 1000));
            Record record = newRecord(i, "key" + i, employee);
            record.setIndexes(mapIndexService.getIndexValues(employee), mapIndexService.getIndexTypes());
            mapIndexService.index(record);
        }
        System.out.println("done");
        long total = Runtime.getRuntime().totalMemory();
        long free = Runtime.getRuntime().freeMemory();
        System.out.println("Used Memory:" + ((total - free) / 1024 / 1024));
        for (int i = 0; i < 10000; i++) {
            long start = System.currentTimeMillis();
            QueryContext queryContext = new QueryContext("default", new SqlPredicate("age >20 and age <23"));
            Set<MapEntry> results = mapIndexService.doQuery(queryContext);
            System.out.println("result size " + results.size() + " took " + (System.currentTimeMillis() - start));
        }
    }
}
