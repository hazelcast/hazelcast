/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test.archunit;

import com.tngtech.archunit.core.domain.JavaClass;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ModuleImportOptionsTest {
    @Test
    void testGetCurrentModuleClasses() {
        Class<?> clazz = ArchUnitRules.class;
        assertClassesContains(ModuleImportOptions.getCurrentModuleClasses(clazz.getPackageName()), clazz);
    }

    @Test
    void testGetCurrentModuleTestClasses() {
        Class<?> clazz = getClass();
        assertClassesContains(ModuleImportOptions.getCurrentModuleTestClasses(clazz.getPackageName()), clazz);
    }

    private static void assertClassesContains(Iterable<JavaClass> classes, Class<?> classToFind) {
        assertThat(classes).extracting(JavaClass::getName)
                .contains(classToFind.getName());
    }
}
