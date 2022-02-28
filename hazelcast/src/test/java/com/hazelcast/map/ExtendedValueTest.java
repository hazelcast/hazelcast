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

package com.hazelcast.map;

import com.hazelcast.map.EntryLoader.MetadataAwareValue;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.map.EntryLoader.MetadataAwareValue.NO_TIME_SET;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({ParallelJVMTest.class, QuickTest.class})
public class ExtendedValueTest {

    @Test(expected = IllegalArgumentException.class)
    public void testNullValueIsNotAccepted() {
        new MetadataAwareValue<>(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullValueIsNotAccepted_withExpiration() {
        new MetadataAwareValue<>(null, -1);
    }

    @Test
    public void testWithNoDate() {
        MetadataAwareValue entry = new MetadataAwareValue<>("value");
        assertEquals(NO_TIME_SET, entry.getExpirationTime());
    }
}
