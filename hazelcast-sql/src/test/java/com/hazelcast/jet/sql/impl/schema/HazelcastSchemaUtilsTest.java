/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.jet.sql.impl.TestTableResolver;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.sql.impl.QueryUtils.prepareSearchPaths;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HazelcastSchemaUtilsTest {
    @Test
    public void testSearchPaths() {
        List<List<String>> paths = prepareSearchPaths(null, null);

        checkSearchPaths(paths);

        paths = prepareSearchPaths(
                Collections.singletonList(Arrays.asList(QueryUtils.CATALOG, "test1")),
                null
        );

        checkSearchPaths(paths, "test1");

        paths = prepareSearchPaths(
                null,
                Arrays.asList(TestTableResolver.create("test1"), TestTableResolver.create("test2"))
        );

        checkSearchPaths(paths, "test1", "test2");

        paths = prepareSearchPaths(
                Collections.singletonList(Arrays.asList(QueryUtils.CATALOG, "test1")),
                Arrays.asList(TestTableResolver.create("test2"), TestTableResolver.create("test3"))
        );

        checkSearchPaths(paths, "test1", "test2", "test3");
    }

    private static void checkSearchPaths(List<List<String>> paths, String... expectedPaths) {
        List<List<String>> expectedPaths0 = new ArrayList<>();

        if (expectedPaths != null) {
            for (String expectedPath : expectedPaths) {
                expectedPaths0.add(Arrays.asList(QueryUtils.CATALOG, expectedPath));
            }
        }

        expectedPaths0.add(Collections.singletonList(QueryUtils.CATALOG));
        expectedPaths0.add(Collections.emptyList());

        assertEquals(expectedPaths0, paths);
    }
}
