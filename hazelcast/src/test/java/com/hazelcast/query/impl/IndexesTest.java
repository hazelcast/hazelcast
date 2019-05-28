/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.SampleTestObjects.Employee;
import com.hazelcast.query.SampleTestObjects.Value;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.instance.TestUtil.toData;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IndexesTest {

    private final InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();

    @Parameter(0)
    public IndexCopyBehavior copyBehavior;

    @Parameters(name = "copyBehavior: {0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {IndexCopyBehavior.COPY_ON_READ},
                {IndexCopyBehavior.COPY_ON_WRITE},
                {IndexCopyBehavior.NEVER}
        });
    }

    @Test
    public void testAndWithSingleEntry() {
        Indexes indexes = Indexes.newBuilder(serializationService, copyBehavior).build();
        indexes.addOrGetIndex("name", false);
        indexes.addOrGetIndex("age", true);
        indexes.addOrGetIndex("salary", true);
        for (int i = 0; i < 100; i++) {
            Employee employee = new Employee(i + "Name", i % 80, (i % 2 == 0), 100 + (i % 1000));
            indexes.putEntry(new QueryEntry(serializationService, toData(i), employee, newExtractor()), null,
                    Index.OperationSource.USER);
        }
        int count = 10;
        Set<String> ages = new HashSet<String>(count);
        for (int i = 0; i < count; i++) {
            ages.add(String.valueOf(i));
        }
        EntryObject entryObject = new PredicateBuilder().getEntryObject();
        PredicateBuilder predicate =
                entryObject.get("name").equal("0Name").and(entryObject.get("age").in(ages.toArray(new String[0])));
        Set<QueryableEntry> results = indexes.query(predicate);
        assertEquals(1, results.size());
    }

    @Test
    public void testIndex() {
        Indexes indexes = Indexes.newBuilder(serializationService, copyBehavior).build();
        indexes.addOrGetIndex("name", false);
        indexes.addOrGetIndex("age", true);
        indexes.addOrGetIndex("salary", true);
        for (int i = 0; i < 2000; i++) {
            Employee employee = new Employee(i + "Name", i % 80, (i % 2 == 0), 100 + (i % 100));
            indexes.putEntry(new QueryEntry(serializationService, toData(i), employee, newExtractor()), null,
                    Index.OperationSource.USER);
        }

        for (int i = 0; i < 10; i++) {
            SqlPredicate predicate = new SqlPredicate("salary=161 and age >20 and age <23");
            Set<QueryableEntry> results = new HashSet<QueryableEntry>(indexes.query(predicate));
            assertEquals(5, results.size());
        }
    }

    @Test
    public void testIndex2() {
        Indexes indexes = Indexes.newBuilder(serializationService, copyBehavior).build();
        indexes.addOrGetIndex("name", false);
        indexes.putEntry(new QueryEntry(serializationService, toData(1), new Value("abc"), newExtractor()), null,
                Index.OperationSource.USER);
        indexes.putEntry(new QueryEntry(serializationService, toData(2), new Value("xyz"), newExtractor()), null,
                Index.OperationSource.USER);
        indexes.putEntry(new QueryEntry(serializationService, toData(3), new Value("aaa"), newExtractor()), null,
                Index.OperationSource.USER);
        indexes.putEntry(new QueryEntry(serializationService, toData(4), new Value("zzz"), newExtractor()), null,
                Index.OperationSource.USER);
        indexes.putEntry(new QueryEntry(serializationService, toData(5), new Value("klm"), newExtractor()), null,
                Index.OperationSource.USER);
        indexes.putEntry(new QueryEntry(serializationService, toData(6), new Value("prs"), newExtractor()), null,
                Index.OperationSource.USER);
        indexes.putEntry(new QueryEntry(serializationService, toData(7), new Value("prs"), newExtractor()), null,
                Index.OperationSource.USER);
        indexes.putEntry(new QueryEntry(serializationService, toData(8), new Value("def"), newExtractor()), null,
                Index.OperationSource.USER);
        indexes.putEntry(new QueryEntry(serializationService, toData(9), new Value("qwx"), newExtractor()), null,
                Index.OperationSource.USER);
        assertEquals(8, new HashSet<QueryableEntry>(indexes.query(new SqlPredicate("name > 'aac'"))).size());
    }

    protected Extractors newExtractor() {
        return Extractors.newBuilder(serializationService).build();
    }

    /**
     * Imagine we have only keys and nullable values. And we add index for a field of that nullable object.
     * When we execute a query on keys, there should be no returned value from indexing service and it does not
     * throw exception.
     */
    @Test
    public void shouldNotThrowException_withNullValues_whenIndexAddedForValueField() {
        Indexes indexes = Indexes.newBuilder(serializationService, copyBehavior).build();
        indexes.addOrGetIndex("name", false);

        shouldReturnNull_whenQueryingOnKeys(indexes);
    }

    @Test
    public void shouldNotThrowException_withNullValues_whenNoIndexAdded() {
        Indexes indexes = Indexes.newBuilder(serializationService, copyBehavior).build();

        shouldReturnNull_whenQueryingOnKeys(indexes);
    }

    private void shouldReturnNull_whenQueryingOnKeys(Indexes indexes) {
        for (int i = 0; i < 50; i++) {
            // passing null value to QueryEntry
            indexes.putEntry(new QueryEntry(serializationService, toData(i), null, newExtractor()), null,
                    Index.OperationSource.USER);
        }

        Set<QueryableEntry> query = indexes.query(new SqlPredicate("__key > 10 "));

        assertNull("There should be no result", query);
    }

    @Test
    public void shouldNotThrowException_withNullValue_whenIndexAddedForKeyField() {
        Indexes indexes = Indexes.newBuilder(serializationService, copyBehavior).build();
        indexes.addOrGetIndex("__key", false);

        for (int i = 0; i < 100; i++) {
            // passing null value to QueryEntry
            indexes.putEntry(new QueryEntry(serializationService, toData(i), null, newExtractor()), null,
                    Index.OperationSource.USER);
        }

        Set<QueryableEntry> query = indexes.query(new SqlPredicate("__key > 10 "));

        assertEquals(89, query.size());
    }

    @Test
    public void testNoDuplicateIndexes() {
        Indexes indexes = Indexes.newBuilder(serializationService, copyBehavior).build();

        InternalIndex index = indexes.addOrGetIndex("a", false);
        assertNotNull(index);
        assertSame(index, indexes.addOrGetIndex("a", false));

        index = indexes.addOrGetIndex("a, b", false);
        assertNotNull(index);
        assertSame(index, indexes.addOrGetIndex("a, b", false));
        assertSame(index, indexes.addOrGetIndex("this.a, b", false));
    }

}
