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

package com.hazelcast.map.impl;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.map.impl.MapConfigValidator.checkInMemoryFormat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapConfigValidatorTest {

    /**
     * Not supported in open source version, so test is expected to throw exception.
     */
    @Test(expected = IllegalArgumentException.class)
    public void test_checkInMemoryFormat_NATIVE() throws Exception {
        checkInMemoryFormat(InMemoryFormat.NATIVE);
    }

    @Test
    public void test_checkInMemoryFormat_OBJECT() throws Exception {
        checkInMemoryFormat(InMemoryFormat.OBJECT);
    }

    @Test
    public void test_checkInMemoryFormat_BINARY() throws Exception {
        checkInMemoryFormat(InMemoryFormat.BINARY);
    }
}