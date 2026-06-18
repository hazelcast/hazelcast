/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector;

import com.hazelcast.config.Config;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.core.HazelcastInstance;
import org.junit.Test;


import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class VectorCollectionLegacyTest {
    @Test
    public void shouldAddConfigAndFetchCollection() {
        HazelcastInstance instance = mock(HazelcastInstance.class);
        Config config = mock(Config.class);
        VectorCollectionConfig collectionConfig = new VectorCollectionConfig("test");
        VectorCollection<Object, Object> expected = mock(VectorCollection.class);
        when(instance.getConfig()).thenReturn(config);
        when(instance.getVectorCollection("test")).thenReturn(expected);
        VectorCollection<Object, Object> result = VectorCollection.getCollection(instance, collectionConfig);
        verify(config).addVectorCollectionConfig(collectionConfig);
        verify(instance).getVectorCollection("test");
        assertThat(result).isSameAs(expected);
    }

    @Test
    public void shouldDelegateToHazelcastInstance() {
        HazelcastInstance instance = mock(HazelcastInstance.class);
        VectorCollection<Object, Object> expected = mock(VectorCollection.class);
        when(instance.getVectorCollection("name")).thenReturn(expected);
        VectorCollection<Object, Object> result = VectorCollection.getCollection(instance, "name");
        verify(instance).getVectorCollection("name");
        assertThat(result).isSameAs(expected);
    }
}
