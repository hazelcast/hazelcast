/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.query;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.MapEntry;
import com.hazelcast.impl.CMap;
import com.hazelcast.util.Clock;
import com.hazelcast.impl.Record;
import com.hazelcast.impl.TestUtil;
import org.junit.*;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class MapIndexServiceTest extends TestUtil {

    @BeforeClass
    @AfterClass
    public static void init() throws Exception {
        Hazelcast.shutdownAll();
    }

    @After
    @Before
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testAndWithSingleEntry() throws Exception {
        CMap cmap = mockCMap("default");
        MapIndexService mapIndexService = new MapIndexService(false);
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
        for (int i = 0; i < 20000; i++) {
            Employee employee = new Employee(i + "Name", i % 80, (i % 2 == 0), 100 + (i % 1000));
            Record record = newRecord(cmap, i, "key" + i, employee);
            record.setIndexes(mapIndexService.getIndexValues(employee), mapIndexService.getIndexTypes());
            mapIndexService.index(record);
        }
        int count = 1000;
        Set<String> ages = new HashSet<String>(count);
        for (int i = 0; i < count; i++) {
            ages.add(String.valueOf(i));
        }
        final EntryObject entryObject = new PredicateBuilder().getEntryObject();
        final PredicateBuilder predicate = entryObject.get("name").equal("140Name").and(entryObject.get("age").in(ages.toArray(new String[0])));
        long total = Runtime.getRuntime().totalMemory();
        long free = Runtime.getRuntime().freeMemory();
        System.out.println("Used Memory:" + ((total - free) / 1024 / 1024));
        for (int i = 0; i < 10000; i++) {
            long start = Clock.currentTimeMillis();
            QueryContext queryContext = new QueryContext("default", predicate, mapIndexService);
            Set<MapEntry> results = mapIndexService.doQuery(queryContext);
//            System.out.println("result size " + results.size() + " took " + (Clock.currentTimeMillis() - start));
            assertEquals(1, results.size());
        }
        cmap.getNode().connectionManager.shutdown();
    }

    @Test
    public void testIndex() throws Exception {
        CMap cmap = mockCMap("default");
        MapIndexService mapIndexService = new MapIndexService(false);
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
        for (int i = 0; i < 20000; i++) {
            Employee employee = new Employee(i + "Name", i % 80, (i % 2 == 0), 100 + (i % 1000));
            Record record = newRecord(cmap, i, "key" + i, employee);
            record.setIndexes(mapIndexService.getIndexValues(employee), mapIndexService.getIndexTypes());
            mapIndexService.index(record);
        }
        long total = Runtime.getRuntime().totalMemory();
        long free = Runtime.getRuntime().freeMemory();
        System.out.println("Used Memory:" + ((total - free) / 1024 / 1024));
        for (int i = 0; i < 10000; i++) {
            long start = Clock.currentTimeMillis();
            QueryContext queryContext = new QueryContext("default", new SqlPredicate("salary=161 and age >20 and age <23"), mapIndexService);
            Set<MapEntry> results = mapIndexService.doQuery(queryContext);
//            for (MapEntry result : results) {
//                System.out.println(((Record)result).getRecordEntry().getValue());
//            }
//            System.out.println("result size " + results.size() + " took " + (Clock.currentTimeMillis() - start));
            assertEquals(10, results.size());
        }
        cmap.getNode().connectionManager.shutdown();
    }
}
