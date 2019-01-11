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

/**
 * Interface for YAML document loader implementations
 * <p/>
 * This interface and its implementations are meant to hide the actual
 * loading logic from the {@link YamlLoader}.
 *
 * @see ReflectiveYamlDocumentLoader
 */
interface YamlDocumentLoader {

    /**
     * Loads the YAML document from the provided {@link InputStream}
     *
     * @param yamlStream The input stream to load the YAML document from
     * @return the root of the loaded YAML document
     * @throws NullPointerException if the provided {@link InputStream} is null
     */
    Object loadFromInputStream(InputStream yamlStream);

    /**
     * Loads the YAML document from the provided {@link Reader}
     *
     * @param yamlReader The reader to load the YAML document from
     * @return the root of the loaded YAML document
     * @throws NullPointerException if the provided {@link Reader} is null
     */
    Object loadFromReader(Reader yamlReader);

    /**
     * Loads the YAML document from the provided {@link String}
     *
     * @param yaml The string to load the YAML document from
     * @return the root of the loaded YAML document
     * @throws NullPointerException if the provided {@link String} is null
     */
    Object loadFromString(String yaml);
}
