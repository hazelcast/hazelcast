/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;

import java.nio.file.Paths;
import java.util.regex.Pattern;

public final class ModuleImportOptions {
    private ModuleImportOptions() {
    }

    private static String getModuleName() {
        return Paths.get("")
                .toAbsolutePath()
                .getFileName()
                .toString();
    }

    private static ImportOption onlyCurrentModule() {
        Pattern projectModulePattern = Pattern.compile(".*/" + getModuleName() + "/target/classes/.*");
        return location -> location.matches(projectModulePattern);
    }

    public static JavaClasses getCurrentModuleClasses(String basePackage) {
        return new ClassFileImporter().withImportOption(onlyCurrentModule())
                .importPackages(basePackage);
    }

    private static ImportOption onlyCurrentModuleTests() {
        Pattern projectModulePattern = Pattern.compile(".*/" + getModuleName() + "/target/test-classes/.*");
        return location -> location.matches(projectModulePattern);
    }

    public static JavaClasses getCurrentModuleTestClasses(String basePackage) {
        return new ClassFileImporter().withImportOption(onlyCurrentModuleTests())
                .importPackages(basePackage);
    }

}
