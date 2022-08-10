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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class KeyedWatermarkCoalescerTest extends JetTestSupport {
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private KeyedWatermarkCoalescer kwc = new KeyedWatermarkCoalescer(2);

    @Test
    public void when_Q1ProducesWmPlusEventAndQ2IsIdle_then_forwardWmAndEventFromQ1() {
        kwc.observeWm(0, wm(0));
        assertEquals(singletonList(wm(0)), kwc.observeWm(1, IDLE_MESSAGE));

        kwc.observeEvent(0);
        assertEquals(singletonList(wm(1)), kwc.observeWm(0, wm(1)));
    }
}
