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

package com.hazelcast.internal.util.collection;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;

import static com.hazelcast.internal.util.collection.ArrayUtils.replaceFirst;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
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

    @Test
    public void replace_whenInMiddle() {
        Integer[] result = replaceFirst(new Integer[]{1, 6, 4}, 6, new Integer[]{2, 3});
        System.out.println(Arrays.toString(result));
        assertArrayEquals(new Integer[]{1, 2, 3, 4}, result);
    }

    @Test
    public void replace_whenInBeginning() {
        Integer[] result = replaceFirst(new Integer[]{6, 3, 4}, 6, new Integer[]{1, 2});
        System.out.println(Arrays.toString(result));
        assertArrayEquals(new Integer[]{1, 2, 3, 4}, result);
    }

    @Test
    public void replace_whenInEnd() {
        Integer[] result = replaceFirst(new Integer[]{1, 2, 6}, 6, new Integer[]{3, 4});
        System.out.println(Arrays.toString(result));
        assertArrayEquals(new Integer[]{1, 2, 3, 4}, result);
    }

    @Test
    public void replace_whenSrcEmpty() {
        Integer[] result = replaceFirst(new Integer[]{}, 6, new Integer[]{3, 4});
        System.out.println(Arrays.toString(result));
        assertArrayEquals(new Integer[]{}, result);
    }

    @Test
    public void replace_whenNewValuesEmpty() {
        Integer[] result = replaceFirst(new Integer[]{1, 2, 10, 3, 4}, 10, new Integer[]{});
        System.out.println(Arrays.toString(result));
        assertArrayEquals(new Integer[]{1, 2, 3, 4}, result);
    }

    @Test
    public void replace_whenNotFound() {
        Integer[] result = replaceFirst(new Integer[]{1, 2, 3}, 10, new Integer[]{100});
        assertArrayEquals(new Integer[]{1, 2, 3}, result);
    }

    @Test
    public void concat() {
        Integer[] first = new Integer[]{1, 2, 3};
        Integer[] second = new Integer[]{4};
        Integer[] concatenated = new Integer[4];
        ArrayUtils.concat(first, second, concatenated);
        assertEquals(4, concatenated.length);
        assertEquals(Integer.valueOf(1), concatenated[0]);
        assertEquals(Integer.valueOf(2), concatenated[1]);
        assertEquals(Integer.valueOf(3), concatenated[2]);
        assertEquals(Integer.valueOf(4), concatenated[3]);
    }

    @Test(expected = NullPointerException.class)
    public void concat_whenFirstNull() {
        Integer[] first = null;
        Integer[] second = new Integer[]{4};
        Integer[] concatenated = new Integer[4];
        ArrayUtils.concat(first, second, concatenated);
        fail();
    }

    @Test(expected = NullPointerException.class)
    public void concat_whenSecondNull() {
        Integer[] first = new Integer[]{1, 2, 3};
        Integer[] second = null;
        Integer[] concatenated = new Integer[4];
        ArrayUtils.concat(first, second, concatenated);
        fail();
    }

    @Test(expected = NullPointerException.class)
    public void concat_whenDestNull() {
        Integer[] first = new Integer[]{1, 2, 3};
        Integer[] second = new Integer[]{4};
        Integer[] concatenated = null;
        ArrayUtils.concat(first, second, concatenated);
        fail();
    }

    @Test
    public void boundsCheck() {
        ArrayUtils.boundsCheck(100, 0, 10);
    }

    @Test
    public void boundsCheck_allZero() {
        ArrayUtils.boundsCheck(0, 0, 0);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void boundsCheck_whenMoreThanCapacity() {
        ArrayUtils.boundsCheck(100, 0, 110);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void boundsCheck_whenIndexSmallerThanZero() {
        ArrayUtils.boundsCheck(100, -1, 110);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void boundsCheck_whenLengthSmallerThanZero() {
        ArrayUtils.boundsCheck(100, 0, -1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void boundsCheck_whenCapacitySmallerThanZero() {
        ArrayUtils.boundsCheck(-1, 0, 0);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void boundsCheck_whenLengthIntegerMax() {
        //Testing wrapping does not cause false check
        ArrayUtils.boundsCheck(0, 10, Integer.MAX_VALUE);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void boundsCheck_whenIndexIntegerMax() {
        //Testing wrapping does not cause false check
        ArrayUtils.boundsCheck(100, Integer.MAX_VALUE, 1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void boundsCheck_whenCapacityIntegerMin() {
        ArrayUtils.boundsCheck(Integer.MIN_VALUE, 0, 100);
    }
}
