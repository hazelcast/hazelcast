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

package com.hazelcast.core;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class DistributedObjectUtilTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() {
        assertUtilityConstructor(DistributedObjectUtil.class);
    }

    @Test
    public void testGetName() {
        DistributedObject distributedObject = mock(DistributedObject.class);
        when(distributedObject.getName()).thenReturn("MockedDistributedObject");

        String name = DistributedObjectUtil.getName(distributedObject);

        assertEquals("MockedDistributedObject", name);
        verify(distributedObject).getName();
        verifyNoMoreInteractions(distributedObject);
    }

    @Test
    public void testGetName_withPrefixedDistributedObject() {
        PrefixedDistributedObject distributedObject = mock(PrefixedDistributedObject.class);
        when(distributedObject.getPrefixedName()).thenReturn("MockedPrefixedDistributedObject");

        String name = DistributedObjectUtil.getName(distributedObject);

        assertEquals("MockedPrefixedDistributedObject", name);
        verify(distributedObject).getPrefixedName();
        verifyNoMoreInteractions(distributedObject);
    }
}
