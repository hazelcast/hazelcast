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

public class JVMTest {

    @Test
    public void getMajorVersion() {
        System.out.println(JVM.getMajorVersion());
        assertEquals(6, JVM.parseVersionString("1.6.0_23"));
        assertEquals(7, JVM.parseVersionString("1.7.0"));
        assertEquals(7, JVM.parseVersionString("1.7.0_80"));
        assertEquals(8, JVM.parseVersionString("1.8.0_211"));
        assertEquals(9, JVM.parseVersionString("9.0.1"));
        assertEquals(11, JVM.parseVersionString("11.0.4"));
        assertEquals(12, JVM.parseVersionString("12"));
        assertEquals(12, JVM.parseVersionString("12.0.1"));
    }
}
