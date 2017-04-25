/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.DistributedFunctions.entryKey;
import static com.hazelcast.jet.DistributedFunctions.entryValue;
import static com.hazelcast.jet.DistributedFunctions.noopConsumer;
import static com.hazelcast.jet.DistributedFunctions.wholeItem;
import static com.hazelcast.jet.Util.entry;
import static org.junit.Assert.*;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class DistributedFunctionsTest {

    @Test
    public void when_wholeItem() {
        Object o = new Object();
        assertSame(o, wholeItem().apply(o));
    }

    @Test
    public void when_entryKey() {
        assertEquals(1, entryKey().apply(entry(1, 2)));
    }

    @Test
    public void when_entryValue() {
        assertEquals(2, entryValue().apply(entry(1, 2)));
    }

    @Test
    public void when_noopConsumer() {
        // assert it's non-null and doesn't fail
        noopConsumer().accept(1);
    }
}
