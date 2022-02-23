/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.schema;

import com.hazelcast.sql.impl.optimizer.PlanObjectKey;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlCatalogTest {
    @Test
    public void when_sameNameInTwoSchemas_then_conflict() {
        // When
        Table s1t1 = new MockTable("s1", "t1");
        Table s1t2 = new MockTable("s1", "t2");
        Table s2t2 = new MockTable("s2", "t2");

        TableResolver tr1 = new MockTableResolver(s1t1, s1t2, s2t2);
        new SqlCatalog(singletonList(tr1));

        // Then
        assertEquals(emptySet(), s1t1.getConflictingSchemas());
        assertEquals(new HashSet<>(asList("s1", "s2")), s1t2.getConflictingSchemas());
        assertEquals(new HashSet<>(asList("s1", "s2")), s2t2.getConflictingSchemas());
    }

    @Test
    public void when_sameFqn_then_noConflict() {
        Table s1t1_1 = new MockTable("s1", "t1");
        Table s1t1_2 = new MockTable("s1", "t1");

        // When
        TableResolver tr1 = new MockTableResolver(s1t1_1, s1t1_2);
        new SqlCatalog(singletonList(tr1));

        // Then
        assertEquals(emptySet(), s1t1_1.getConflictingSchemas());
        assertEquals(emptySet(), s1t1_2.getConflictingSchemas());
    }

    @Test
    public void when_sameFqnAndConflict_then_conflict() {
        Table s1t1_1 = new MockTable("s1", "t1");
        Table s1t1_2 = new MockTable("s1", "t1");
        Table s2t1 = new MockTable("s2", "t1");

        // When
        TableResolver tr1 = new MockTableResolver(s1t1_1);
        TableResolver tr2 = new MockTableResolver(s1t1_2, s2t1);
        new SqlCatalog(asList(tr1, tr2));

        // Then
        Set<String> bothSchemas = new HashSet<>(asList("s1", "s2"));
        assertEquals(bothSchemas, s1t1_1.getConflictingSchemas());
        // the second occurrence of s1t1 doesn't have the conflicts set
        assertEquals(emptySet(), s1t1_2.getConflictingSchemas());
        assertEquals(bothSchemas, s2t1.getConflictingSchemas());
    }

    private static class MockTableResolver implements TableResolver {
        private final List<Table> tables;

        private MockTableResolver(Table... tables) {
            this.tables = asList(tables);
        }

        @Nonnull
        @Override
        public List<List<String>> getDefaultSearchPaths() {
            return emptyList();
        }

        @Nonnull
        @Override
        public List<Table> getTables() {
            return tables;
        }

        @Override
        public void registerListener(TableListener listener) {
        }
    }

    private static class MockTable extends Table {
        MockTable(String schema, String tableName) {
            super(schema, tableName, emptyList(), null);
        }

        @Override
        public PlanObjectKey getObjectKey() {
            return null;
        }
    }
}

