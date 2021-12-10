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

import com.hazelcast.jet.impl.util.EnumerationUtil;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;

final class ClassScanner {
    private static final char PATH_SEPARATOR = '/';

    private ClassScanner() {
    }

    static List<String> findClassFiles(JarFile file, String simpleClassName) {
        String filename = simpleClassName + ".class";
        return EnumerationUtil.stream(file.entries())
                .map(ZipEntry::getName)
                .map(Paths::get)
                .filter(byFilename(filename))
                .map(Path::toString)
                .collect(Collectors.toList());
    }

    private static Predicate<Path> byFilename(String filename) {
        return path -> path.getFileName().toString().equals(filename);
    }

    static List<String> findClassFiles(Class<?> clazz) {
        String resourceName = clazz.getName().replace('.', PATH_SEPARATOR) + ".class";
        try {
            return Collections.list(clazz.getClassLoader().getResources(resourceName))
                    .stream()
                    .map(URL::getPath)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
