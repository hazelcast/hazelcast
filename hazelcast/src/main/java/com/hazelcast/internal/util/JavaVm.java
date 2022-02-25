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

import java.util.Properties;

/**
 * Utility for checking runtime Java VM.
 *
 */
public enum JavaVm {
    UNKNOWN,
    HOTSPOT,
    OPENJ9;

    public static final JavaVm CURRENT_VM = detectCurrentVM();

    private static JavaVm detectCurrentVM() {
        Properties properties = System.getProperties();
        return parse(properties);
    }

    static JavaVm parse(Properties properties) {
        String vmName = properties.getProperty("java.vm.name");
        if (vmName == null) {
            return UNKNOWN;
        }
        // Oracle JDK 'java.vm.name': Java HotSpot(TM) 64-Bit Server VM
        // Oracle JDK 'sun.management.compiler': HotSpot 64-Bit Tiered Compilers
        if (vmName.contains("HotSpot")) {
            return HOTSPOT;
        }
        // Eclipse OpenJ9 'java.vm.name': Eclipse OpenJ9 VM
        if (vmName.contains("OpenJ9")) {
            return OPENJ9;
        }
        String prop = properties.getProperty("sun.management.compiler");
        if (prop == null) {
            return UNKNOWN;
        }
        // HotSpot based OpenJDK builds 'java.vm.name': OpenJDK 64-Bit Server VM.
        // HotSpot based OpenJDK builds 'sun.management.compiler': HotSpot 64-Bit Tiered Compilers
        if (prop.contains("HotSpot")) {
            return HOTSPOT;
        }
        return UNKNOWN;
    }
}
