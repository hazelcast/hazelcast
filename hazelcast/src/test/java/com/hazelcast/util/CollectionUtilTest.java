/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.collection.ArrayUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CollectionUtilTest {

    @Test
    public void getItemAtPositionOrNull_whenEmptyArray_thenReturnNull() {
        Collection<Object> src = new LinkedHashSet<Object>();
        Object result = CollectionUtil.getItemAtPositionOrNull(src, 0);

        assertNull(result);
    }

    @Test
    public void getItemAtPositionOrNull_whenPositionExist_thenReturnTheItem() {
        Object obj = new Object();
        Collection<Object> src = new LinkedHashSet<Object>();
        src.add(obj);

        Object result = CollectionUtil.getItemAtPositionOrNull(src, 0);

        assertSame(obj, result);
    }

    @Test
    public void getItemAtPositionOrNull_whenSmallerArray_thenReturnNull() {
        Object obj = new Object();
        Collection<Object> src = new LinkedHashSet<Object>();
        src.add(obj);

        Object result = CollectionUtil.getItemAtPositionOrNull(src, 1);

        assertNull(result);
    }

    @Test
    public void getItemAsPositionOrNull_whenInputImplementsList_thenDoNotUserIterator() {
        Object obj = new Object();

        List<Object> src = mock(List.class);
        when(src.size()).thenReturn(1);
        when(src.get(0)).thenReturn(obj);

        Object result = CollectionUtil.getItemAtPositionOrNull(src, 0);

        assertSame(obj, result);
        verify(src, never()).iterator();
    }
}
