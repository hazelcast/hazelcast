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

package com.hazelcast.instance.impl;

import com.hazelcast.config.IntegrityCheckerConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.util.ServiceLoader;
import com.hazelcast.logging.ILogger;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;

import java.util.List;
import java.util.stream.Collectors;

import static com.hazelcast.jet.impl.util.Util.CONFIG_CHANGE_TEMPLATE;
import static java.lang.String.format;

public final class IntegrityChecker {
    private static final String FACTORY_ID = "com.hazelcast.DataSerializerHook";
    private static final String INTEGRITY_CHECKER_IS_DISABLED = "Integrity Checker is disabled. "
            + "Fail-fast on corrupted executables will not be performed. "
            + "For more information, see the documentation for Integrity Checker.";
    private static final String INTEGRITY_CHECKER_IS_ENABLED = "Starting Integrity Check scan. "
            + "This is a costly operation and it can be disabled if startup time is important. \n"
            + "To disable Integrity Checker do one of the following: \n"
            + format(CONFIG_CHANGE_TEMPLATE,
            "config.setIntegrityCheckerEnabled(false);",
            "hazelcast.integrity-checker.enabled to false",
            "-Dhz.integritychecker.enabled=false",
            "HZ_INTEGRITYCHECKER_ENABLED=false"
    );

    private final ILogger logger;
    private final IntegrityCheckerConfig config;

    public IntegrityChecker(final IntegrityCheckerConfig config, final ILogger logger) {
        this.logger = logger;
        this.config = config;
    }

    public void checkIntegrity() {
        if (!config.isEnabled()) {
            logger.info(INTEGRITY_CHECKER_IS_DISABLED);
            return;
        }

        logger.info(INTEGRITY_CHECKER_IS_ENABLED);
        final long start = System.nanoTime();
        final List<String> classNames = new ClassGraph()
                .enableClassInfo()
                .scan()
                .getClassesImplementing(DataSerializerHook.class)
                .stream()
                .map(ClassInfo::getName)
                .collect(Collectors.toList());

        final long scanTime = (System.nanoTime() - start) / 1000_000L;
        logger.info(format("Integrity Check scan finished in %d milliseconds", scanTime));

        checkModule(classNames);

        final long totalTime = (System.nanoTime() - start) / 1000_000L;
        logger.info(format("Integrity Check finished in %d milliseconds", totalTime));
    }

    private void checkModule(final List<String> hooks) {
        for (String className : hooks) {
            try {
                final Class<?> hookClass = getClassLoader().loadClass(className);
                final Object hook = ServiceLoader.load(
                        hookClass,
                        FACTORY_ID,
                        getClassLoader()
                );

                if (hook == null) {
                    throw new HazelcastException("Failed to instantiate DataSerializerHook class instance: "
                            + className);
                }
            } catch (ClassNotFoundException classNotFoundException) {
                throw new HazelcastException("Failed to verify distribution integrity, "
                        + "unable to load DataSerializerHook class: " + className, classNotFoundException);
            } catch (Exception e) {
                throw new HazelcastException(format("Failed to verify distribution integrity, "
                        + "unable to load DataSerializerHook: %s", className));
            }
        }
    }

    private ClassLoader getClassLoader() {
        return IntegrityChecker.class.getClassLoader();
    }
}
