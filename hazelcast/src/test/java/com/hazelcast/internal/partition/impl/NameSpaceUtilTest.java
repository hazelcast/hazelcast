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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.internal.services.DistributedObjectNamespace;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NameSpaceUtilTest {

    private static final String SERVICE_NAME = "service";
    private Map<Integer, Integer> containers;

    @Before
    public void setup() {
        containers = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            containers.put(i, i);
        }
    }

    @Test
    public void testGetAllNamespaces_whenAllMatch() {
        Collection<ServiceNamespace> namespaces = NameSpaceUtil.getAllNamespaces(containers, container -> true,
                container -> new DistributedObjectNamespace(SERVICE_NAME, Integer.toString(container)));
        assertEquals(containers.size(), namespaces.size());
    }

    @Test
    public void testGetAllNamespaces_whenOneMatches() {
        Collection<ServiceNamespace> namespaces =
                NameSpaceUtil.getAllNamespaces(containers,
                container -> container == 5,
                container -> new DistributedObjectNamespace(SERVICE_NAME, Integer.toString(container)));
        assertEquals(1, namespaces.size());
    }

    @Test
    public void testGetAllNamespaces_namespacesMutable() {
        Collection<ServiceNamespace> namespaces =
                NameSpaceUtil.getAllNamespaces(containers,
                container -> container == 5,
                container -> new DistributedObjectNamespace(SERVICE_NAME, Integer.toString(container)));
        ObjectNamespace namespaceToRetain = new DistributedObjectNamespace(SERVICE_NAME, Integer.toString(6));
        namespaces.retainAll(Collections.singleton(namespaceToRetain));
        assertEquals(0, namespaces.size());
    }
}
