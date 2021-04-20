/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.jet.SimpleTestInClusterSupport;
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
public class MappingStorageTest extends SimpleTestInClusterSupport {

    private MappingStorage storage;

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
    }

    @Before
    public void before() {
        storage = new MappingStorage(nodeEngine(instance().getHazelcastInstance()));
    }

    @Test
    public void when_put_then_isPresentInValues() {
        String name = randomName();

        Mapping previous = storage.put(name, mapping(name, "type"));

        assertThat(previous).isNull();
        assertThat(storage.values().stream().filter(m -> m.name().equals(name))).isNotEmpty();
    }

    @Test
    public void when_put_then_overridesPrevious() {
        String name = randomName();
        Mapping originalMapping = mapping(name, "type1");
        Mapping updatedMapping = mapping(name, "type2");

        storage.put(name, originalMapping);
        Mapping previous = storage.put(name, updatedMapping);

        assertThat(previous).isEqualTo(originalMapping);
        assertThat(storage.values().stream().filter(m -> m.equals(originalMapping))).isEmpty();
        assertThat(storage.values().stream().filter(m -> m.equals(updatedMapping))).isNotEmpty();
    }

    @Test
    public void when_putIfAbsent_then_doesNotOverride() {
        String name = randomName();

        assertThat(storage.putIfAbsent(name, mapping(name, "type-1"))).isTrue();
        assertThat(storage.putIfAbsent(name, mapping(name, "type-2"))).isFalse();
        assertThat(storage.values().stream().filter(m -> m.type().equals("type-1"))).isNotEmpty();
        assertThat(storage.values().stream().filter(m -> m.type().equals("type-2"))).isEmpty();
    }

    @Test
    public void when_remove_then_isNotPresentInValues() {
        String name = randomName();

        storage.put(name, mapping(name, "type"));

        assertThat(storage.remove(name)).isNotNull();
        assertThat(storage.values().stream().filter(m -> m.name().equals(name))).isEmpty();
    }

    @Test
    public void when_removeAbsentValue_then_returnsNull() {
        assertThat(storage.remove("non-existing")).isNull();
    }

    private static Mapping mapping(String name, String type) {
        return new Mapping(name, name, type, emptyList(), emptyMap());
    }
}
