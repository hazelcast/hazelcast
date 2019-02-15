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

package com.hazelcast.jet.function;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.function.DistributedFunctions.entryValue;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static com.hazelcast.jet.function.DistributedPredicate.alwaysFalse;
import static com.hazelcast.jet.function.DistributedPredicate.alwaysTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastSerialClassRunner.class)
public class DistributedFunctionsTest extends HazelcastTestSupport {

    @Test
    public void constructor() {
        assertUtilityConstructor(DistributedFunctions.class);
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
        DistributedConsumer.noop().accept(1);
    }
}
