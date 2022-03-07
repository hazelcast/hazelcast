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

import com.hazelcast.core.Hazelcast;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static com.hazelcast.internal.util.ModularJavaUtils.PackageAccessRequirement.Mode.EXPORT;
import static com.hazelcast.internal.util.ModularJavaUtils.PackageAccessRequirement.Mode.OPEN;
import static com.hazelcast.internal.util.ModularJavaUtils.PackageAccessRequirement.export;
import static com.hazelcast.internal.util.ModularJavaUtils.PackageAccessRequirement.open;
import static com.hazelcast.internal.util.ModularJavaUtils.PackageAccessRequirement.packages;

/**
 * Helper class for simplify work with Java module system (Java 9+) in older Java versions.
 */
public final class ModularJavaUtils {

    private static final ILogger LOGGER = Logger.getLogger(ModularJavaUtils.class);
    private static final PackageAccessRequirement[] NO_REQUIREMENTS = new PackageAccessRequirement[0];

    private ModularJavaUtils() {
    }

    public static String getHazelcastModuleName() {
        if (!JavaVersion.isAtLeast(JavaVersion.JAVA_9)) {
            return null;
        }
        try {
            return getName(hazelcastModule());
        } catch (Exception e) {
            LOGGER.finest("Getting Hazelcast module name failed", e);
            return null;
        }
    }

    private static Class<?> moduleClass() throws ClassNotFoundException {
        return Class.forName("java.lang.Module");
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

        Map<String, PackageAccessRequirement[]> moduleRequirements = new TreeMap<>();
        moduleRequirements.put("java.base",
                packages(
                        export("jdk.internal.ref"),
                        open("java.lang"),
                        open("sun.nio.ch")
                )
        );
        moduleRequirements.put("jdk.management", getJdkManagementRequirements());
        moduleRequirements.put("java.management", packages(open("sun.management")));
        checkPackageRequirements(logger, moduleRequirements);
    }

    private static PackageAccessRequirement[] getJdkManagementRequirements() {
        if (JavaVm.CURRENT_VM == JavaVm.OPENJ9) {
            return packages(
                    open("com.sun.management.internal"),
                    export("com.ibm.lang.management.internal")
            );
        }
        return packages(open("com.sun.management.internal"));
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
            for (Object module : modules()) {
                PackageAccessRequirement[] packageRequirements = requirements.getOrDefault(getName(module), NO_REQUIREMENTS);

                for (PackageAccessRequirement req : packageRequirements) {
                    boolean hasAccess = req.isForReflection()
                            ? isOpen(module, req.getPackageName())
                            : isExported(module, req.getPackageName());
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

    private static String getName(Object module) throws ReflectiveOperationException {
        Method methodGetName = moduleClass().getMethod("getName");
        return (String) methodGetName.invoke(module);
    }

    private static boolean isExported(Object module, String packageName) throws ReflectiveOperationException {
        Method methodIsExported = moduleClass().getMethod("isExported", String.class, moduleClass());
        return (boolean) methodIsExported.invoke(module, packageName, hazelcastModule());
    }

    private static boolean isOpen(Object module, String packageName) throws ReflectiveOperationException {
        Method methodIsOpen = moduleClass().getMethod("isOpen", String.class, moduleClass());
        return (boolean) methodIsOpen.invoke(module, packageName, hazelcastModule());
    }

    private static Object hazelcastModule() throws ReflectiveOperationException {
        Method methodGetModule = Class.class.getMethod("getModule");
        return methodGetModule.invoke(Hazelcast.class);
    }

    private static Set<?> modules() throws ReflectiveOperationException {
        Class<?> classModuleLayer = Class.forName("java.lang.ModuleLayer");
        Method methodBoot = classModuleLayer.getMethod("boot");
        Method methodModules = classModuleLayer.getMethod("modules");
        Object moduleLayerBoot = methodBoot.invoke(null);
        return (Set<?>) methodModules.invoke(moduleLayerBoot);
    }

    private static String createOpenPackageJavaArguments(String hzModuleName,
                                                         Map<String, PackageAccessRequirement[]> requirements) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, PackageAccessRequirement[]> moduleEntry : requirements.entrySet()) {
            for (PackageAccessRequirement requirement : moduleEntry.getValue()) {
                sb.append(requirement.isForReflection() ? " --add-opens " : " --add-exports ").append(moduleEntry.getKey())
                        .append("/").append(requirement.packageName).append("=").append(hzModuleName);
            }
        }
        return sb.toString();
    }

    static final class PackageAccessRequirement {
        private final String packageName;
        private final Mode mode;

        private PackageAccessRequirement(Mode mode, String packageName) {
            this.packageName = packageName;
            this.mode = mode;
        }

        static PackageAccessRequirement[] packages(PackageAccessRequirement... requirements) {
            return requirements;
        }

        static PackageAccessRequirement open(String packageName) {
            return new PackageAccessRequirement(OPEN, packageName);
        }

        static PackageAccessRequirement export(String packageName) {
            return new PackageAccessRequirement(EXPORT, packageName);
        }

        @Override
        public String toString() {
            return "PackageAccessRequirement{"
                    + "packageName='" + packageName + '\''
                    + ", mode=" + mode
                    + '}';
        }

        String getPackageName() {
            return packageName;
        }

        boolean isForReflection() {
            return mode == OPEN;
        }

        enum Mode {
            OPEN, EXPORT
        }
    }

}
