/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ProgressIndicatorTest {

    @Test
    public void test() {
        ProgressIndicator progressIndicator = new ProgressIndicator();
        assertEquals(0, progressIndicator.get());

        assertEquals(1, progressIndicator.inc());
        assertEquals(1, progressIndicator.get());

        assertEquals(2, progressIndicator.inc());
        assertEquals(2, progressIndicator.get());

        assertEquals(12, progressIndicator.inc(10));
        assertEquals(12, progressIndicator.get());

        assertEquals(12, progressIndicator.inc(0));
        assertEquals(12, progressIndicator.get());

        assertEquals(11, progressIndicator.inc(-1));
        assertEquals(11, progressIndicator.get());
    }

    @Test
    public void test_toString() {
        ProgressIndicator progressIndicator = new ProgressIndicator();
        progressIndicator.inc(100);

        assertEquals("100", progressIndicator.toString());
    }
}
