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

package com.hazelcast.sql.impl.extract;

import com.hazelcast.sql.impl.CoreSqlTestSupport;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.sql.impl.extract.QueryPath.KEY;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryPathTest extends CoreSqlTestSupport {
    @Test
    public void testPathResolution() {
        // Top fields.
        checkPath(KEY, true, null);
        checkPath(VALUE, false, null);

        // Simple paths.
        checkPath("attr", false, "attr");
        checkPath("attr1.attr2", false, "attr1.attr2");

        // Prefixed paths.
        checkPath(KEY + ".attr", true, "attr");
        checkPath(KEY + ".attr1.attr2", true, "attr1.attr2");
        checkPath(VALUE + ".attr", false, "attr");
        checkPath(VALUE + ".attr1.attr2", false, "attr1.attr2");

        // Test exceptions.
        checkFails(null);
        checkFails("");
        checkFails(KEY + ".");
        checkFails(VALUE + ".");
    }

    @Test
    public void testSerialization() {
        QueryPath original = new QueryPath("test", true);
        QueryPath restored = serializeAndCheck(original, SqlDataSerializerHook.QUERY_PATH);

        assertEquals(original.getPath(), restored.getPath());
        assertEquals(original.isKey(), restored.isKey());
    }

    private void checkFails(String path) {
        assertThrows(QueryException.class, () -> QueryPath.create(path));
    }

    private void checkPath(String originalPath, boolean expectedKey, String expectedPath) {
        QueryPath path = QueryPath.create(originalPath);

        assertEquals(expectedKey, path.isKey());
        assertEquals(expectedPath, path.getPath());

        assertEquals(expectedPath == null, path.isTop());
    }
}
