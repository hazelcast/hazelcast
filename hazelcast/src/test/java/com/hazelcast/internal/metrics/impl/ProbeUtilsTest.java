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

package com.hazelcast.internal.metrics.impl;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.metrics.impl.ProbeUtils.TYPE_COLLECTION;
import static com.hazelcast.internal.metrics.impl.ProbeUtils.TYPE_COUNTER;
import static com.hazelcast.internal.metrics.impl.ProbeUtils.TYPE_DOUBLE_NUMBER;
import static com.hazelcast.internal.metrics.impl.ProbeUtils.TYPE_DOUBLE_PRIMITIVE;
import static com.hazelcast.internal.metrics.impl.ProbeUtils.TYPE_LONG_NUMBER;
import static com.hazelcast.internal.metrics.impl.ProbeUtils.TYPE_MAP;
import static com.hazelcast.internal.metrics.impl.ProbeUtils.TYPE_PRIMITIVE_LONG;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ProbeUtilsTest extends HazelcastTestSupport {

    @Test
    public void testPrivateConstructor() {
        assertUtilityConstructor(ProbeUtils.class);
    }

    @Test
    public void isDouble() {
        assertTrue(ProbeUtils.isDouble(TYPE_DOUBLE_NUMBER));
        assertTrue(ProbeUtils.isDouble(TYPE_DOUBLE_PRIMITIVE));

        assertFalse(ProbeUtils.isDouble(TYPE_PRIMITIVE_LONG));
        assertFalse(ProbeUtils.isDouble(TYPE_LONG_NUMBER));
        assertFalse(ProbeUtils.isDouble(TYPE_COLLECTION));
        assertFalse(ProbeUtils.isDouble(TYPE_MAP));
        assertFalse(ProbeUtils.isDouble(TYPE_COUNTER));
    }
}
