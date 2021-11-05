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

import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.sql.impl.schema.Mapping;
import com.hazelcast.sql.impl.schema.view.View;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static com.hazelcast.sql.impl.SqlTestSupport.nodeEngine;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

@Category({QuickTest.class, ParallelJVMTest.class})
public class ViewsStorageTest extends SimpleTestInClusterSupport {

    private static final String QUERY = "SELECT 1";
    private static final String QUERY2 = "SELECT 2";

    private ViewStorage storage;

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
    }

    @Before
    public void before() {
        storage = new ViewStorage(nodeEngine(instance()));
    }

    @Test
    public void when_put_then_isPresentInValues() {
        String name = randomName();

        storage.put(name, view(name, "type"));

        assertThat(storage.values().stream().filter(m -> m.name().equals(name))).isNotEmpty();
    }

    @Test
    public void when_put_then_overridesPrevious() {
        String name = randomName();
        View originalView = view(name, QUERY);
        View updatedView = view(name, QUERY2);

        storage.put(name, originalView);
        storage.put(name, updatedView);

        assertThat(storage.values().stream().filter(m -> m.equals(originalView))).isEmpty();
        assertThat(storage.values().stream().filter(m -> m.equals(updatedView))).isNotEmpty();
    }

    @Test
    public void when_putIfAbsent_then_doesNotOverride() {
        String name = randomName();

        assertThat(storage.putIfAbsent(name, view(name, QUERY))).isTrue();
        assertThat(storage.putIfAbsent(name, view(name, QUERY2))).isFalse();
        assertThat(storage.values().stream().filter(m -> m.query().equals(QUERY))).isNotEmpty();
        assertThat(storage.values().stream().filter(m -> m.query().equals(QUERY2))).isEmpty();
    }

    @Test
    public void when_remove_then_isNotPresentInValues() {
        String name = randomName();

        storage.put(name, view(name, QUERY));

        assertThat(storage.remove(name)).isNotNull();
        assertThat(storage.values().stream().filter(m -> m.name().equals(name))).isEmpty();
    }

    @Test
    public void when_removeAbsentValue_then_returnsNull() {
        assertThat(storage.remove("non-existing")).isNull();
    }

    private static View view(String name, String query) {
        return new View(name, query);
    }
}
