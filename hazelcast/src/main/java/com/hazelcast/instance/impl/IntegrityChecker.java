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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.util.ServiceLoader;
import com.hazelcast.logging.ILogger;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfoList;

import java.util.List;

import static java.lang.String.format;

public final class IntegrityChecker {
    private static final String FACTORY_ID = "com.hazelcast.DataSerializerHook";
    private final ILogger logger;

    public IntegrityChecker(final ILogger logger) {
        this.logger = logger;
    }

    public void checkIntegrity() {
        logger.info("WARNING: Integrity checker is enabled, this is a costly operation and it is advised to disable "
                + "Integrity Checker for non-development environments. "
                + "To disable Integrity Checker set hazelcast.integrity-checker.enabled "
                + "to false in the member configuration.");

        logger.info("Starting Integrity Check Scan");
        final long start = System.nanoTime();
        final ClassInfoList classes = new ClassGraph()
                .enableClassInfo()
                .scan()
                .getClassesImplementing(DataSerializerHook.class);

        final long scanTime = (System.nanoTime() - start) / 1000_000L;
        logger.info(format("Integrity Check scan finished in %d milliseconds", scanTime));

        checkModule(classes.getAsStrings());

        final long totalTime = (System.nanoTime() - start) / 1000_000L;
        logger.info(format("Integrity Check finished in %d milliseconds", totalTime));
    }

    private void checkModule(final List<String> hooks) {
        for (String className : hooks) {
            final Class<?> hookClass;
            try {
                hookClass = getClassLoader().loadClass(className);
            } catch (ClassNotFoundException ignored) {
                continue;
            }

            try {
                final Object hook = ServiceLoader.load(
                        hookClass,
                        FACTORY_ID,
                        getClassLoader()
                );

                if (hook == null) {
                    throw new HazelcastException("Failed to instantiate DataSerializerHook class instance");
                }
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
