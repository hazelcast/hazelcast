/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.Hazelcast;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static com.hazelcast.internal.util.ModularJavaUtils.PackageAccessRequirement.createRequirement;

/**
 * Helper class for simplify work with Java module system (Java 9+) in older Java versions.
 */
public final class ModularJavaUtils {

    private static final ILogger LOGGER = Logger.getLogger(ModularJavaUtils.class);

    private ModularJavaUtils() {
    }

    public static String getHazelcastModuleName() {
        if (!JavaVersion.isAtLeast(JavaVersion.JAVA_9)) {
            return null;
        }
        try {
            Method methodGetModule = Class.class.getMethod("getModule");
            Class<?> classModule = Class.forName("java.lang.Module");
            Method methodGetName = classModule.getMethod("getName");
            Object moduleHazelcast = methodGetModule.invoke(Hazelcast.class);
            return (String) methodGetName.invoke(moduleHazelcast);
        } catch (Exception e) {
            LOGGER.finest("Getting Hazelcast module name failed", e);
            return null;
        }
    }

    /**
     * Prints warning to given {@link ILogger} if Hazelcast is not provided a sufficient access to Java internal packages on
     * Java 9 and newer.
     */
    public static void checkJavaInternalAccess(ILogger logger) {
        if (logger == null || !JavaVersion.isAtLeast(JavaVersion.JAVA_9)) {
            // older Java versions are fine with the reflection
            return;
        }

        Map<String, PackageAccessRequirement[]> requirements = new TreeMap<String, PackageAccessRequirement[]>();
        requirements.put("java.base",
                new PackageAccessRequirement[] {
                        createRequirement(false, "jdk.internal.ref"),
                        createRequirement(true, "java.lang"),
                        createRequirement(true, "java.nio"),
                        createRequirement(true, "sun.nio.ch")
                        });
        requirements.put("jdk.management", getJdkManagementRequirements());
        requirements.put("java.management", new PackageAccessRequirement[] { createRequirement(true, "sun.management") });
        checkPackageRequirements(logger, requirements);
    }

    private static PackageAccessRequirement[] getJdkManagementRequirements() {
        if (JavaVm.CURRENT_VM == JavaVm.OPENJ9) {
            return new PackageAccessRequirement[] {createRequirement(true, "com.sun.management.internal"),
                                                   createRequirement(true, "com.ibm.lang.management.internal")};
        }
        return new PackageAccessRequirement[] {createRequirement(true, "com.sun.management.internal")};
    }

    static void checkPackageRequirements(ILogger logger, Map<String, PackageAccessRequirement[]> requirements) {
        if (!hasHazelcastPackageAccess(requirements)) {
            String hazelcastModule = getHazelcastModuleName();
            if (hazelcastModule == null) {
                hazelcastModule = "ALL-UNNAMED";
            }
            logger.warning("Hazelcast is starting in a Java modular environment (Java 9 and newer)"
                    + " but without proper access to required Java packages."
                    + " Use additional Java arguments to provide Hazelcast access to Java internal API."
                    + " The internal API access is used to get the best performance results. Arguments to be used:\n"
                    + " --add-modules java.se"  + createOpenPackageJavaArguments(hazelcastModule, requirements));
        }
    }

    private static boolean hasHazelcastPackageAccess(Map<String, PackageAccessRequirement[]> requirements) {
        try {
            Class<?> classModuleLayer = Class.forName("java.lang.ModuleLayer");
            Class<?> classModule = Class.forName("java.lang.Module");
            Method methodGetModule = Class.class.getMethod("getModule");
            Method methodBoot = classModuleLayer.getMethod("boot");
            Method methodModules = classModuleLayer.getMethod("modules");

            Method methodGetName = classModule.getMethod("getName");
            Method methodIsOpen = classModule.getMethod("isOpen", String.class, classModule);
            Method methodIsExported = classModule.getMethod("isExported", String.class, classModule);

            Object moduleHazelcast = methodGetModule.invoke(Hazelcast.class);
            Object moduleLayerBoot = methodBoot.invoke(null);
            Set<?> moduleSet = (Set<?>) methodModules.invoke(moduleLayerBoot);
            for (Object m : moduleSet) {
                PackageAccessRequirement[] reqArray = requirements.get(methodGetName.invoke(m));
                if (reqArray == null) {
                    continue;
                }
                for (PackageAccessRequirement req : reqArray) {
                    Method methodToCheck = req.isForReflection() ? methodIsOpen : methodIsExported;
                    boolean hasAccess = (Boolean) methodToCheck.invoke(m, req.getPackageName(), moduleHazelcast);
                    if (!hasAccess) {
                        return false;
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.finest("Checking Hazelcast package access", e);
            return false;
        }
        return true;
    }

    private static String createOpenPackageJavaArguments(String hzModuleName,
            Map<String, PackageAccessRequirement[]> requirements) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, PackageAccessRequirement[]> moduleEntry : requirements.entrySet()) {
            for (PackageAccessRequirement requirement : moduleEntry.getValue()) {
                sb.append(requirement.forReflection ? " --add-opens " : " --add-exports ").append(moduleEntry.getKey())
                        .append("/").append(requirement.packageName).append("=").append(hzModuleName);
            }
        }
        return sb.toString();
    }

    static final class PackageAccessRequirement {
        private final String packageName;
        private final boolean forReflection;

        private PackageAccessRequirement(boolean forReflection, String packageName) {
            this.packageName = packageName;
            this.forReflection = forReflection;
        }

        static PackageAccessRequirement createRequirement(boolean forReflection, String packageName) {
            return new PackageAccessRequirement(forReflection, packageName);
        }

        String getPackageName() {
            return packageName;
        }

        boolean isForReflection() {
            return forReflection;
        }
    }
}
