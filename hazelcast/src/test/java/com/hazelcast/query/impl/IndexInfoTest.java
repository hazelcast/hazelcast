/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.assumeDifferentHashCodes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IndexInfoTest {

    private IndexInfo indexInfo;
    private IndexInfo indexInfoSameAttributes;

    private IndexInfo indexInfoOtherIsOrdered;
    private IndexInfo indexInfoOtherAttributeName;
    private IndexInfo indexInfoNullAttributeName;

    @Before
    public void setUp() {
        indexInfo = new IndexInfo("foo", true);
        indexInfoSameAttributes = new IndexInfo("foo", true);

        indexInfoOtherIsOrdered = new IndexInfo("foo", false);
        indexInfoOtherAttributeName = new IndexInfo("bar", true);
        indexInfoNullAttributeName = new IndexInfo(null, true);
    }

    @Test
    public void testEquals() {
        assertEquals(indexInfo, indexInfo);
        assertEquals(indexInfo, indexInfoSameAttributes);

        assertNotEquals(indexInfo, null);
        assertNotEquals(indexInfo, new Object());

        assertNotEquals(indexInfo, indexInfoOtherIsOrdered);
        assertNotEquals(indexInfo, indexInfoOtherAttributeName);
        assertNotEquals(indexInfo, indexInfoNullAttributeName);
    }

    @Test
    public void testHashCode() {
        assertEquals(indexInfo.hashCode(), indexInfo.hashCode());
        assertEquals(indexInfo.hashCode(), indexInfoSameAttributes.hashCode());

        assumeDifferentHashCodes();
        assertNotEquals(indexInfo.hashCode(), indexInfoOtherIsOrdered.hashCode());
        assertNotEquals(indexInfo.hashCode(), indexInfoOtherAttributeName.hashCode());
        assertNotEquals(indexInfo.hashCode(), indexInfoNullAttributeName.hashCode());
    }


    @Test
    public void testCompareTo() {
        assertEquals(0, indexInfo.compareTo(indexInfoSameAttributes));

        assertEquals(1, indexInfo.compareTo(indexInfoOtherIsOrdered));
        assertEquals(-1, indexInfoOtherIsOrdered.compareTo(indexInfo));

        assertEquals(4, indexInfo.compareTo(indexInfoOtherAttributeName));
        assertEquals(-4, indexInfoOtherAttributeName.compareTo(indexInfo));
    }
}
