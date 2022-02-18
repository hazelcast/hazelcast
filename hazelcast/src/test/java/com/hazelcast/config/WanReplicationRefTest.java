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

package com.hazelcast.config;

import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WanReplicationRefTest {

    private WanReplicationRef ref = new WanReplicationRef();

    @Test
    public void testConstructor_withParameters() {
        ref = new WanReplicationRef("myRef", "myMergePolicy", singletonList("myFilter"), true);

        assertEquals("myRef", ref.getName());
        assertEquals("myMergePolicy", ref.getMergePolicyClassName());
        assertEquals(1, ref.getFilters().size());
        assertEquals("myFilter", ref.getFilters().get(0));
        assertTrue(ref.isRepublishingEnabled());
    }

    @Test
    public void testConstructor_withWanReplicationRef() {
        WanReplicationRef original = new WanReplicationRef("myRef", "myMergePolicy", singletonList("myFilter"), true);
        ref = new WanReplicationRef(original);

        assertEquals(original.getName(), ref.getName());
        assertEquals(original.getMergePolicyClassName(), ref.getMergePolicyClassName());
        assertEquals(original.getFilters(), ref.getFilters());
        assertEquals(original.isRepublishingEnabled(), ref.isRepublishingEnabled());
    }

    @Test
    public void testSetFilters() {
        List<String> filters = new ArrayList<String>();
        filters.add("myFilter1");
        filters.add("myFilter2");

        ref.setFilters(filters);

        assertEquals(2, ref.getFilters().size());
        assertEquals("myFilter1", ref.getFilters().get(0));
        assertEquals("myFilter2", ref.getFilters().get(1));
    }

    @Test
    public void testSerialization() {
        ref.setName("myWanReplicationRef");
        ref.setMergePolicyClassName("myMergePolicy");
        ref.setRepublishingEnabled(true);
        ref.addFilter("myFilter");

        SerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        Data serialized = serializationService.toData(ref);
        WanReplicationRef deserialized = serializationService.toObject(serialized);

        assertEquals(ref.getName(), deserialized.getName());
        assertEquals(ref.getMergePolicyClassName(), deserialized.getMergePolicyClassName());
        assertEquals(ref.isRepublishingEnabled(), deserialized.isRepublishingEnabled());
        assertEquals(ref.getFilters(), deserialized.getFilters());
        assertEquals(ref.toString(), deserialized.toString());
    }
}
