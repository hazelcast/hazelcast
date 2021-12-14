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
import com.hazelcast.internal.util.ServiceLoader;
import com.hazelcast.internal.yaml.YamlLoader;
import com.hazelcast.internal.yaml.YamlMapping;
import com.hazelcast.internal.yaml.YamlNode;
import com.hazelcast.internal.yaml.YamlScalar;
import com.hazelcast.internal.yaml.YamlSequence;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

public final class ModuleIntegrityChecker {
    private static final String CONFIG_PATH = "/integrity-checker-config.yaml";
    private static final String FACTORY_ID = "com.hazelcast.DataSerializerHook";

    private ModuleIntegrityChecker() { }

    public static void checkIntegrity() {
        final InputStream config = ModuleIntegrityChecker.class
                .getResourceAsStream(CONFIG_PATH);

        if (config == null) {
            throw new HazelcastException("Failed to load integrity checker config file");
        }

        final YamlMapping root = (YamlMapping) YamlLoader.load(config);

        for (final YamlNode moduleNode : root.childAsMapping("modules").children()) {
           final String moduleName = moduleNode.nodeName();
           final List<String> moduleHooks = new ArrayList<>();
           ((YamlSequence) moduleNode).children()
                    .forEach(node -> moduleHooks.add(((YamlScalar) node).nodeValue()));

           checkModule(moduleName, moduleHooks);
        }
    }

    private static void checkModule(String moduleName, List<String> hooks) {
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
                throw new HazelcastException(format("Failed to verify module[%s] integrity, "
                        + "unable to load DataSerializerHook: %s", moduleName, className));
            }
        }
    }

    private static ClassLoader getClassLoader() {
        return ModuleIntegrityChecker.class.getClassLoader();
    }
}
