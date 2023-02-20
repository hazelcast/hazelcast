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

package com.hazelcast.instance.impl;

import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.IOException;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

class MainClassFinder {

    private static final ILogger LOGGER = Logger.getLogger(MainClassFinder.class);

    boolean calledByMember;
    private String errorMessage;

    private String mainClassName;

    String getErrorMessage() {
        return errorMessage;
    }

    String getMainClassName() {
        return mainClassName;
    }

    // Find the mainClass to be used to execute the jar
    void findMainClass(String mainClass, String jarPath, boolean calledByMember)
            throws IOException {

        this.calledByMember = calledByMember;

        try (JarFile jarFile = new JarFile(jarPath)) {
            checkHazelcastCodebasePresence(jarFile);

            // Assume that mainClass is provided
            this.mainClassName = mainClass;

            // If the given parameter is null, try to read mainClassName from the jar manifest
            if (StringUtil.isNullOrEmpty(mainClassName)) {

                Manifest manifest = jarFile.getManifest();
                if (manifest == null) {
                    this.errorMessage = "No manifest file in the jar";
                    return;
                }
                Attributes mainAttributes = manifest.getMainAttributes();
                this.mainClassName = mainAttributes.getValue("Main-Class");
                if (this.mainClassName == null) {
                    errorMessage = "No Main-Class found in the manifest of the jar";
                }
            }
        }
    }

    private void checkHazelcastCodebasePresence(JarFile jarFile) {
        List<String> classFiles = JarScanner.findClassFiles(jarFile, HazelcastBootstrap.class.getSimpleName());
        if (!classFiles.isEmpty()) {
            String message = String.format("WARNING: Hazelcast code detected in the jar: %s. "
                                         + "Hazelcast dependency should be set with the 'provided' scope or equivalent.%n",
                    String.join(", ", classFiles));
            if (calledByMember) {
                LOGGER.info(message);
            } else {
                System.err.print(message);
            }
        }
    }
}
