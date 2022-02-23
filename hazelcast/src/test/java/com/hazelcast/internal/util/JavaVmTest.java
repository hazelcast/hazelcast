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
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;

import static org.junit.Assert.assertEquals;


@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JavaVmTest {

    @Test
    public void parseOracleJdkProps() {
        Properties props = new Properties();
        props.put("java.vm.name", "Java HotSpot(TM) 64-Bit Server VM");
        props.put("sun.management.compiler", "HotSpot 64-Bit Tiered Compilers");

        JavaVm javaVm = JavaVm.parse(props);
        assertEquals(JavaVm.HOTSPOT, javaVm);
    }

    @Test
    public void parseZuluOpenJdkProps() {
        Properties props = new Properties();
        props.put("java.vm.name", "OpenJDK 64-Bit Server VM");
        props.put("sun.management.compiler", "HotSpot 64-Bit Tiered Compilers");

        JavaVm javaVm = JavaVm.parse(props);
        assertEquals(JavaVm.HOTSPOT, javaVm);
    }

    @Test
    public void parseEclipseOpenJdkProps() {
        Properties props = new Properties();
        props.put("java.vm.name", "OpenJDK 64-Bit Server VM");
        props.put("sun.management.compiler", "HotSpot 64-Bit Tiered Compilers");

        JavaVm javaVm = JavaVm.parse(props);
        assertEquals(JavaVm.HOTSPOT, javaVm);
    }

    @Test
    public void parseEclipseOpenJ9OpenJdkProps() {
        Properties props = new Properties();
        props.put("java.vm.name", "Eclipse OpenJ9 VM");

        JavaVm javaVm = JavaVm.parse(props);
        assertEquals(JavaVm.OPENJ9, javaVm);
    }
}
