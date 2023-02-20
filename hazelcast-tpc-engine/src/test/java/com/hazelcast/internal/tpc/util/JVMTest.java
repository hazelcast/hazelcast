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

package com.hazelcast.internal.tpc.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class JVMTest {

    @Test
    public void getMajorVersion() {
        System.out.println(JVM.getMajorVersion());
        assertEquals(JVM.parseVersionString("1.6.0_23"), 6);
        assertEquals(JVM.parseVersionString("1.7.0"), 7);
        assertEquals(JVM.parseVersionString("1.7.0_80"), 7);
        assertEquals(JVM.parseVersionString("1.8.0_211"), 8);
        assertEquals(JVM.parseVersionString("9.0.1"), 9);
        assertEquals(JVM.parseVersionString("11.0.4"), 11);
        assertEquals(JVM.parseVersionString("12"), 12);
        assertEquals(JVM.parseVersionString("12.0.1"), 12);
    }
}
