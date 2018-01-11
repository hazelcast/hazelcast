/*
 * Copyright (c) 2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import static org.junit.Assert.assertSame;

import java.util.Locale;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.config.MultiMapConfig.ValueCollectionType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;

/**
 * Tests for {@link MultiMapConfig} class.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MultiMapConfigTest {

    @Test
    public void testValueCollectionTypeSelection() {
        Locale locale = Locale.getDefault();
        MultiMapConfig multiMapConfig = new MultiMapConfig();
        multiMapConfig.setValueCollectionType("list");
        try {
            assertSame(ValueCollectionType.LIST, multiMapConfig.getValueCollectionType());
            Locale.setDefault(new Locale("tr"));
            assertSame(ValueCollectionType.LIST, multiMapConfig.getValueCollectionType());
        } finally {
            Locale.setDefault(locale);
        }
    }
}
