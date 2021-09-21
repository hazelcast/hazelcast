/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.tngtech.archunit.core.importer.ImportOption;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class ModuleImportOptions {

    private ModuleImportOptions() {
    }

    public static ImportOption onlyCurrentModule() {
        String callerClassName = new Throwable().getStackTrace()[1].getClassName();
        String moduleName = getProjectModuleName(callerClassName);
        Pattern projectModulePattern = Pattern.compile(".*/" + moduleName + "/target/classes/.*");
        return location -> location.matches(projectModulePattern);
    }

    private static String getProjectModuleName(String clazz) {
        String currentClassLocation = getClassLocation(clazz);
        Pattern moduleExtractingPattern = Pattern.compile(".*/([^/]+)/target/test-classes/.*");
        Matcher matcher = moduleExtractingPattern.matcher(currentClassLocation);
        if (matcher.find()) {
            return matcher.group(1);
        }
        throw new IllegalStateException("Current module not found");
    }

    private static String getClassLocation(String className) {
        try {
            Class<?> clazz = Class.forName(className);
            return clazz.getProtectionDomain().getCodeSource().getLocation().getPath();

        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
