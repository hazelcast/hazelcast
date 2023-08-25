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

package com.hazelcast.instance.impl.executejar;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Predicate;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;

final class JarScanner {
    private JarScanner() {
    }

    static List<String> findClassFiles(JarFile jarFile, String className) {
        String filename = className + ".class";
        return jarFile.stream()
                .map(ZipEntry::getName)
                .map(Paths::get)
                .filter(byFilename(filename))
                .map(Path::toString)
                .collect(Collectors.toList());
    }

    private static Predicate<Path> byFilename(String filename) {
        // ignore the package name because the HZ classes could be shaded in the jar
        return path -> path.getFileName().toString().equals(filename);
    }
}
