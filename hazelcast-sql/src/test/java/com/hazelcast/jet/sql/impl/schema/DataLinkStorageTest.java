/*
 * Copyright 2023 Hazelcast Inc.
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
import com.hazelcast.sql.impl.schema.datalink.DataLink;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

@Category({QuickTest.class, ParallelJVMTest.class})
public class DataLinkStorageTest extends SimpleTestInClusterSupport {
    private DataLinkStorage storage;

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
    }

    @Before
    public void before() {
        storage = new DataLinkStorage(Accessors.getNodeEngineImpl(instance()));
    }

    @Test
    public void when_put_then_isPresentInValues() {
        String name = randomName();

        storage.put(name, dataLink(name, "type"));

        assertThat(storage.dataLinkNames().stream().filter(m -> m.equals(name))).isNotEmpty();
    }

    @Test
    public void when_put_then_overridesPrevious() {
        String name = randomName();
        DataLink originalDL = dataLink(name, "type1");
        DataLink updatedDL = dataLink(name, "type2");

        storage.put(name, originalDL);
        storage.put(name, updatedDL);

        assertTrue(storage.allObjects().stream().noneMatch(dl -> dl.equals(originalDL)));
        assertTrue(storage.allObjects().stream().anyMatch(dl -> dl.equals(updatedDL)));
    }

    @Test
    public void when_putIfAbsent_then_doesNotOverride() {
        String name = randomName();

        assertThat(storage.putIfAbsent(name, dataLink(name, "type-1"))).isTrue();
        assertThat(storage.putIfAbsent(name, dataLink(name, "type-2"))).isFalse();
        assertTrue(storage.allObjects().stream()
                .anyMatch(dl -> dl instanceof DataLink && ((DataLink) dl).getType().equals("type-1")));
        assertTrue(storage.allObjects().stream()
                .noneMatch(dl -> dl instanceof DataLink && ((DataLink) dl).getType().equals("type-2")));
    }

    @Test
    public void when_removeMapping_then_isNotPresentInValues() {
        String name = randomName();

        storage.put(name, dataLink(name, "type"));

        assertThat(storage.removeDataLink(name)).isNotNull();
        assertTrue(storage.dataLinkNames().stream().noneMatch(dl -> dl.equals(name)));
    }

    private static DataLink dataLink(String name, String type) {
        return new DataLink(name, type, emptyMap(), false);
    }
}
