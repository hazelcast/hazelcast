/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpc.server;

import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.Enumeration;

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;

/**
 * Loads all ops registered in file "META-INF/services/com.hazelcast.Commands" into the
 * {@link CmdRegistry}.
 */
public final class CmdLoader {

    private static final String resourceName = "META-INF/services/com.hazelcast.Commands";

    private final ClassLoader classLoader;
    private final CmdRegistry commandRegistry;

    public CmdLoader(CmdRegistry commandRegistry, ClassLoader classLoader) {
        this.commandRegistry = checkNotNull(commandRegistry, "commandRegistry");
        this.classLoader = checkNotNull(classLoader, "classLoader");
    }

    public void load() {
        try {
            Enumeration<URL> resources = classLoader.getResources(resourceName);
            while (resources.hasMoreElements()) {
                load(resources.nextElement());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void load(URL resource) throws Exception {
        try (BufferedReader reader = new BufferedReader(new FileReader(resource.getFile()))) {
            String line = reader.readLine();
            while (line != null) {
                line = line.trim();
                boolean skip = line.startsWith("#") || line.isEmpty();
                if (!skip) {
                    Class clazz = classLoader.loadClass(line);
                    try {
                        Field idField = clazz.getField("ID");

                        // more validation can be done on the field like static, type, public etc.
                        commandRegistry.register((Byte) idField.get(null), clazz);
                    } catch (NoSuchFieldException e) {
                        throw new RuntimeException("Could not find field 'ID' on class" + clazz.getName());
                    }
                }

                line = reader.readLine();
            }
        }
    }
}
