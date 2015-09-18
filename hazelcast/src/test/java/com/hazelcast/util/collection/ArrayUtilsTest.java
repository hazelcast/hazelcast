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

package com.hazelcast.util.collection;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ArrayUtilsTest {

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
}
