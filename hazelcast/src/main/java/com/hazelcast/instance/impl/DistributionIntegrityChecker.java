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

package com.hazelcast.instance.impl;

import java.util.Arrays;
import java.util.List;

public final class DistributionIntegrityChecker {
    private static final List<String> MODULE_CHECKER_CLASS_NAMES = Arrays.asList(
            "com.hazelcast.jet.sql.impl.JetSqlModuleIntegrityChecker"
    );

    private DistributionIntegrityChecker() { }

    public static void checkModulesIntegrity() {
        for (String className : MODULE_CHECKER_CLASS_NAMES) {
            final ModuleIntegrityChecker checker = loadIntegrityChecker(className);
            if (checker != null) {
                checker.check();
            }
        }
    }

    private static ModuleIntegrityChecker loadIntegrityChecker(final String className) {
        try {
            return (ModuleIntegrityChecker) Class.forName(className).newInstance();
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException ignored) {
            return null;
        }
    }
}
