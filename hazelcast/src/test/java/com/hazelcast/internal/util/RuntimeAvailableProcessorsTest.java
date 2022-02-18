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

package com.hazelcast.internal.util;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.assertUtilityConstructor;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class RuntimeAvailableProcessorsTest {

    @Test
    public void getAvailableProcessors_withoutOverride() throws Exception {
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        assertEquals(availableProcessors, RuntimeAvailableProcessors.get());
    }

    @Test
    public void getAvailableProcessors_withOverride() throws Exception {
        int customAvailableProcessors = 1234;
        RuntimeAvailableProcessors.override(customAvailableProcessors);
        try {
            assertEquals(customAvailableProcessors, RuntimeAvailableProcessors.get());
        } finally {
            RuntimeAvailableProcessors.resetOverride();
        }
    }

    @Test
    public void testConstructor() {
        assertUtilityConstructor(RuntimeAvailableProcessors.class);
    }

}
