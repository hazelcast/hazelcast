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

package com.hazelcast.internal.yaml;

import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * {@link YamlDocumentLoader} implementation loading YAML documents via
 * the SnakeYaml Engine library. Since the used library is a Java8+
 * library, this implementation uses reflection to make the source
 * compiling with JDK6. The aim of having this class is to hide the
 * reflective access from {@link YamlLoader} and is meant to be
 * exclusively used by {@link YamlLoader}.
 */
class ReflectiveYamlDocumentLoader implements YamlDocumentLoader {
    private final Object load;
    private final Method loadFromInputStream;
    private final Method loadFromReader;
    private final Method loadFromString;

    ReflectiveYamlDocumentLoader() {
        try {
            // instantiate LoadSettingsBuilder
            Class<?> loadSettingsBuilderClass = Class.forName("org.snakeyaml.engine.v1.api.LoadSettingsBuilder");
            Constructor<?> loadSettingsBuilderConstructor = loadSettingsBuilderClass.getConstructor();
            Object loadSettingsBuilder = loadSettingsBuilderConstructor.newInstance();
            Method buildLoadSettingsMethod = loadSettingsBuilderClass.getMethod("build");

            // instantiate LoadSettings via the builder
            Object loadSettings = buildLoadSettingsMethod.invoke(loadSettingsBuilder);

            // instantiate Load
            Class<?> loadSettingsClass = Class.forName("org.snakeyaml.engine.v1.api.LoadSettings");
            Class<?> loadClass = Class.forName("org.snakeyaml.engine.v1.api.Load");
            Constructor<?> constructor = loadClass.getConstructor(loadSettingsClass);
            load = constructor.newInstance(loadSettings);

            // getting the Method instances for each loadFrom Load methods
            loadFromInputStream = loadClass.getMethod("loadFromInputStream", InputStream.class);
            loadFromReader = loadClass.getMethod("loadFromReader", Reader.class);
            loadFromString = loadClass.getMethod("loadFromString", String.class);
        } catch (Exception e) {
            throw new YamlException("An error occurred while creating the SnakeYaml Load class", e);
        }
    }

    @Override
    public Object loadFromInputStream(InputStream yamlStream) {
        checkNotNull(yamlStream, "The provided InputStream to load the YAML from must not be null");
        try {
            return loadFromInputStream.invoke(load, yamlStream);
        } catch (Exception e) {
            throw new YamlException("Couldn't load YAML document from the provided InputStream", e);
        }
    }

    @Override
    public Object loadFromReader(Reader yamlReader) {
        checkNotNull(yamlReader, "The provided Reader to load the YAML from must not be null");
        try {
            return loadFromReader.invoke(load, yamlReader);
        } catch (Exception e) {
            throw new YamlException("Couldn't load YAML document from the provided Reader", e);
        }
    }

    @Override
    public Object loadFromString(String yaml) {
        checkNotNull(yaml, "The provided String to load the YAML from must not be null");
        try {
            return loadFromString.invoke(load, yaml);
        } catch (Exception e) {
            throw new YamlException("Couldn't load YAML document from the provided String", e);
        }
    }
}
