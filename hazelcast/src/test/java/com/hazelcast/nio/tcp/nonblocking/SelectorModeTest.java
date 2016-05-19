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

package com.hazelcast.nio.tcp.nonblocking;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;

/**
 * Test SelectorMode
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class SelectorModeTest {

    @Test
    public void getConfiguredValue_whenSelectWithFix() throws Exception {
        String originalValue = System.getProperty("hazelcast.io.selectorMode");
        System.setProperty("hazelcast.io.selectorMode", "selectwithfix");
        try {
            assertEquals(SelectorMode.SELECT_WITH_FIX, SelectorMode.getConfiguredValue());
        }
        finally {
            if (originalValue == null) {
                System.clearProperty("hazelcast.io.selectorMode");
            }
            else {
                System.setProperty("hazelcast.io.selectorMode", originalValue);
            }
        }
    }

    @Test
    public void fromString_whenSelectNow() throws Exception {
        assertEquals(SelectorMode.SELECT_NOW, SelectorMode.fromString("selectnow"));
    }

    @Test
    public void fromString_whenSelect() throws Exception {
        assertEquals(SelectorMode.SELECT, SelectorMode.fromString("select"));
    }

    @Test
    public void fromString_whenNull() throws Exception {
        assertEquals(SelectorMode.SELECT, SelectorMode.fromString(null));
    }

}
