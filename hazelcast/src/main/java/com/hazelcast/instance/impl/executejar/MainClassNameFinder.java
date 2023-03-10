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

import com.hazelcast.instance.impl.HazelcastBootstrap;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.IOException;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 * This class is public so that its log output can be tested
 */
public class MainClassNameFinder {

    private static final ILogger LOGGER = Logger.getLogger(MainClassNameFinder.class);

    private String errorMessage;

    private String mainClassName;

    String getErrorMessage() {
        return errorMessage;
    }

    String getMainClassName() {
        return mainClassName;
    }

    boolean hasError() {
        return !StringUtil.isNullOrEmpty(errorMessage);
    }

    // Find the mainClassName from the jar
    public void findMainClass(String jarPath, String mainClassName)
            throws IOException {
        try (JarFile jarFile = new JarFile(jarPath)) {
            checkHazelcastCodebasePresence(jarFile);

            // Assume that mainClassName is provided
            this.mainClassName = mainClassName;

            // If the given parameter is null, try to read mainClassName from the jar manifest
            if (StringUtil.isNullOrEmpty(this.mainClassName)) {

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
            LOGGER.info(message);
        }
    }
}
