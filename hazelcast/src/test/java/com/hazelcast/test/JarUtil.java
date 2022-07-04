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

package com.hazelcast.test;

import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.util.Lists.newArrayList;

/**
 * Util class to work with (create) jar files for tests. Usually used when testing classloading features.
 */
public class JarUtil {

    public static void createResourcesJarFile(File jarFile) {
        JarUtil.createJarFile(
                newArrayList(
                        "childfirstclassloader/resource_jar.txt",
                        "childfirstclassloader/resource_test.txt"
                ),
                newArrayList(
                        "resource in jar".getBytes(UTF_8),
                        "resource in jar".getBytes(UTF_8)
                ),
                jarFile.getAbsolutePath()
        );
    }

    /**
     * Creates a jar file with files taken from the source directory.
     *
     * @param sourceDir the directory with the files
     * @param files     the files to put into the jar
     * @param outputJar the location of the jar file
     */
    public static void createJarFile(String sourceDir, List<String> files, String outputJar) {
        try (FileOutputStream out = new FileOutputStream(outputJar);
             JarOutputStream jarOS = new JarOutputStream(out)) {

            for (String file : files) {
                writeEntry(jarOS, sourceDir, file);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void writeEntry(JarOutputStream jarOS, String sourceFolder, String name) throws IOException {
        jarOS.putNextEntry(new JarEntry(name));
        try (FileInputStream fis = new FileInputStream(sourceFolder + name)) {
            jarOS.write(IOUtils.toByteArray(fis));
        }
        jarOS.closeEntry();
    }

    public static void createJarFile(List<String> fileNames, List<byte[]> data, String outputJar) {
        if (fileNames.size() != data.size()) {
            throw new IllegalArgumentException("fileNames and data must have same size()");
        }
        try (FileOutputStream out = new FileOutputStream(outputJar);
             JarOutputStream jarOS = new JarOutputStream(out)) {

            for (int i = 0; i < fileNames.size(); i++) {
                jarOS.putNextEntry(new JarEntry(fileNames.get(i)));
                jarOS.write(data.get(i));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
