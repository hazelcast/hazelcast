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

package com.hazelcast.function;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.function.Functions.entryKey;
import static com.hazelcast.function.Functions.entryValue;
import static com.hazelcast.function.Functions.wholeItem;
import static com.hazelcast.function.PredicateEx.alwaysFalse;
import static com.hazelcast.function.PredicateEx.alwaysTrue;
import static com.hazelcast.query.impl.predicates.PredicateTestUtils.entry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@Category({QuickTest.class, ParallelJVMTest.class})
@RunWith(HazelcastParallelClassRunner.class)
public class FunctionsTest extends HazelcastTestSupport {

    @Test
    public void constructor() {
        assertUtilityConstructor(Functions.class);
    }

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
    public void when_alwaysTrue() {
        assertEquals(true, alwaysTrue().test(3));
    }

    @Test
    public void when_alwaysFalse() {
        assertEquals(false, alwaysFalse().test(2));
    }

    @Test
    public void when_noopConsumer() {
        // assert it's non-null and doesn't fail
        ConsumerEx.noop().accept(1);
    }
}
