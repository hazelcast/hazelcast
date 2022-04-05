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

package com.hazelcast.map.impl.nearcache.invalidation;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class UuidFilterTest {

    private static UUID uuid = new UUID(0, 0);
    private static UUID otherUuid = new UUID(1, 1);

    private UuidFilter uuidFilter = new UuidFilter(uuid);

    @Test
    public void testEval() {
        assertTrue(uuidFilter.eval(uuid));
    }

    @Test
    public void testEval_withNonMatchingParameter() {
        assertFalse(uuidFilter.eval(otherUuid));
    }

    @RequireAssertEnabled
    @Test(expected = AssertionError.class)
    public void testEval_withInvalidParameter() {
        uuidFilter.eval(5);
    }

    @Test
    public void testToString() {
        assertNotNull(uuidFilter.toString());
    }
}
