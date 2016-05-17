/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util.collection;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ArrayUtilsTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() {
        assertUtilityConstructor(ArrayUtils.class);
    }

    @Test
    public void createCopy_whenZeroLengthArray_thenReturnDifferentZeroLengthArray() {
        Object[] original = new Object[0];
        Object[] result = ArrayUtils.createCopy(original);

        assertThat(result, not(sameInstance(original)));
        assertThat(result, emptyArray());
    }

    @Test
    public void createCopy_whenNonZeroLengthArray_thenReturnDifferentArrayWithItems() {
        Object[] original = new Object[1];
        Object o = new Object();
        original[0] = o;

        Object[] result = ArrayUtils.createCopy(original);
        assertThat(result, not(sameInstance(original)));
        assertThat(result, arrayWithSize(1));
        assertThat(result[0], sameInstance(o));
    }


    @Test
    public void copyWithoutNulls_whenSrcHasNullItem_thenDoNotCopyItIntoTarget() {
        Object[] src = new Object[1];
        src[0] = null;
        Object[] dst = new Object[0];

        ArrayUtils.copyWithoutNulls(src, dst);
        assertThat(dst, emptyArray());
    }

    @Test
    public void copyWithoutNulls_whenSrcHasNonNullItem_thenCopyItIntoTarget() {
        Object[] src = new Object[1];
        Object o = new Object();
        src[0] = o;
        Object[] dst = new Object[1];

        ArrayUtils.copyWithoutNulls(src, dst);
        assertThat(dst, arrayWithSize(1));
        assertThat(dst[0], sameInstance(o));
    }

    @Test
    public void contains() {
        Object[] array = new Object[1];
        Object object = new Object();
        array[0] = object;

        assertTrue(ArrayUtils.contains(array, object));
    }

    @Test
    public void contains_returnsFalse() {
        Object[] array = new Object[1];
        Object object = new Object();
        array[0] = object;

        assertFalse(ArrayUtils.contains(array, new Object()));
    }

    @Test
    public void contains_nullValue() {
        Object[] array = new Object[1];
        array[0] = null;

        assertTrue(ArrayUtils.contains(array, null));
    }

    @Test
    public void contains_nullValue_returnsFalse() {
        Object[] array = new Object[1];
        array[0] = new Object();

        assertFalse(ArrayUtils.contains(array, null));
    }

    @Test
    public void getItemAtPositionOrNull_whenEmptyArray_thenReturnNull() {
        Object[] src = new Object[0];
        Object result = ArrayUtils.getItemAtPositionOrNull(src, 0);

        assertNull(result);
    }

    @Test
    public void getItemAtPositionOrNull_whenPositionExist_thenReturnTheItem() {
        Object obj = new Object();
        Object[] src = new Object[1];
        src[0] = obj;

        Object result = ArrayUtils.getItemAtPositionOrNull(src, 0);

        assertSame(obj, result);
    }

    @Test
    public void getItemAtPositionOrNull_whenSmallerArray_thenReturNull() {
        Object obj = new Object();
        Object[] src = new Object[1];
        src[0] = obj;

        Object result = ArrayUtils.getItemAtPositionOrNull(src, 1);

        assertNull(result);
    }
}
