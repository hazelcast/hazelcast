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

package com.hazelcast.internal.networking.nio;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.channels.Selector;

import static com.hazelcast.internal.networking.nio.SelectorOptimizer.findOptimizableSelectorClass;
import static com.hazelcast.test.HazelcastTestSupport.assertUtilityConstructor;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class SelectorOptimizerTest {
    private ILogger logger = Logger.getLogger(SelectionKeysSetTest.class);

    @Test
    public void testConstructor() {
        assertUtilityConstructor(SelectorOptimizer.class);
    }

    @Test
    public void optimize() throws Exception {
        Selector selector = Selector.open();
        assumeTrue(findOptimizableSelectorClass(selector) != null);
        SelectorOptimizer.SelectionKeysSet keys = SelectorOptimizer.optimize(selector, logger);
        assertNotNull(keys);
    }
}
