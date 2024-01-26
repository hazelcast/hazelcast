/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.instance.GeneratedBuildProperties;
import org.apache.commons.io.FilenameUtils;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;

/**
 * References to classes which are not part of the classpath at test execution time.
 */
public class UserCodeUtil {
    public static final UserCodeUtil INSTANCE = new UserCodeUtil(GeneratedBuildProperties.ARTIFACT_ID, GeneratedBuildProperties.VERSION);

    private String artifactId;
    private String version;

    UserCodeUtil(String artifactId, String version) {
        this.artifactId = artifactId;
        this.version = version;
    }

    /**
     * Not possible to name the files in a sensible way on creation - because the `maven-jar-plugin` only supports a single
     * output name for the entire project - so the classifier is used to distinguish (and to prevent overlap which will confuse
     * maven)
     */
    public String getCompiledJARName(String classifier) {
        return String.join("-", artifactId, version, classifier) + FilenameUtils.EXTENSION_SEPARATOR + "jar";
    }

    public static Path pathRelativeToBinariesFolder(String... path) {
        return Paths.get("target", path);
    }

    public static URL urlRelativeToBinariesFolder(String... path) {
        try {
            return pathRelativeToBinariesFolder(path).toUri().toURL();
        } catch (MalformedURLException e) {
            throw sneakyThrow(e);
        }
    }
}
