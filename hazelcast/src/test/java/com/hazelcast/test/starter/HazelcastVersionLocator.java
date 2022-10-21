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

package com.hazelcast.test.starter;

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.version.Version;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.nio.IOUtil.drainTo;
import static com.hazelcast.test.JenkinsDetector.isOnJenkins;
import static com.hazelcast.test.starter.HazelcastStarterUtils.rethrowGuardianException;
import static java.io.File.separator;
import static java.lang.String.format;

public class HazelcastVersionLocator {
    public enum Artifact {
        OS_JAR("/com/hazelcast/hazelcast/%1$s/hazelcast-%1$s.jar", false, false, "hazelcast"),
        OS_TEST_JAR("/com/hazelcast/hazelcast/%1$s/hazelcast-%1$s-tests.jar", false, true, "hazelcast"),
        SQL_JAR("/com/hazelcast/hazelcast-sql/%1$s/hazelcast-sql-%1$s.jar", false, false, "hazelcast-sql"),
        EE_JAR("/com/hazelcast/hazelcast-enterprise/%1$s/hazelcast-enterprise-%1$s.jar", true, false, "hazelcast-enterprise"),
        EE_TEST_JAR("/com/hazelcast/hazelcast-enterprise/%1$s/hazelcast-enterprise-%1$s-tests.jar", true, true, "hazelcast-enterprise"),
        ;
        private final String path;
        private final boolean enterprise;
        private final boolean test;
        private final String mavenProject;

        Artifact(String path, boolean enterprise, boolean test, String mavenName) {
            this.path = path;
            this.enterprise = enterprise;
            this.test = test;
            this.mavenProject = mavenName;
        }
    }

    private static final ILogger LOGGER = Logger.getLogger(HazelcastVersionLocator.class);

    private static final String LOCAL_M2_REPOSITORY_PREFIX;
    private static final String MAVEN_CENTRAL_PREFIX;
    private static final String HAZELCAST_REPOSITORY_PREFIX;

    static {
        LOCAL_M2_REPOSITORY_PREFIX = System.getProperty("user.home") + separator + ".m2" + separator + "repository";
        MAVEN_CENTRAL_PREFIX = "https://repo1.maven.org/maven2";
        HAZELCAST_REPOSITORY_PREFIX = "https://repository.hazelcast.com/release";
    }

    public static Map<Artifact, File> locateVersion(String version, File target, boolean enterprise) {
        Map<Artifact, File> files = new HashMap<>();
        files.put(Artifact.OS_JAR, locateArtifact(Artifact.OS_JAR, version, target));
        files.put(Artifact.OS_TEST_JAR, locateArtifact(Artifact.OS_TEST_JAR, version, target));
        if (Version.of(version).isGreaterOrEqual(Versions.V5_0)) {
            files.put(Artifact.SQL_JAR, locateArtifact(Artifact.SQL_JAR, version, target));
        }
        if (enterprise) {
            files.put(Artifact.EE_JAR, locateArtifact(Artifact.EE_JAR, version, target));
            files.put(Artifact.EE_TEST_JAR, locateArtifact(Artifact.EE_TEST_JAR, version, target));
        }
        return files;
    }

    private static File locateArtifact(Artifact artifact, String version, File target) {
        String path = format(artifact.path, version);
        File localCopy = new File(LOCAL_M2_REPOSITORY_PREFIX + path);
        if (localCopy.exists()) {
            return localCopy;
        } else {
            return downloadArtifact(artifact, version, target, path);
        }
    }

    private static File downloadArtifact(Artifact artifact, String version, File target, String path) {
        String url = (artifact.enterprise ? HAZELCAST_REPOSITORY_PREFIX : MAVEN_CENTRAL_PREFIX) + path;
        String filename = extractFilenameFromUrl(url);
        logWarningForArtifactDownload(artifact, version);
        return downloadFile(url, target, filename);
    }

    private static String extractFilenameFromUrl(String url) {
        int lastIndexOf = url.lastIndexOf('/');
        return url.substring(lastIndexOf);
    }

    private static File downloadFile(String url, File targetDirectory, String filename) {
        File targetFile = new File(targetDirectory, filename);
        if (targetFile.isFile() && targetFile.exists()) {
            return targetFile;
        }
        FileOutputStream fos = null;
        InputStream is = null;
        try {
            is = new BufferedInputStream(new URL(url).openStream());
            fos = new FileOutputStream(targetFile);
            drainTo(is, fos);
            targetFile.deleteOnExit();
            return targetFile;
        } catch (IOException e) {
            throw rethrowGuardianException(e);
        } finally {
            closeResource(fos);
            closeResource(is);
        }
    }

    private static void logWarningForArtifactDownload(Artifact artifact, String version) {
        if (isOnJenkins()) {
            return;
        }

        StringBuilder sb = new StringBuilder();
        sb.append("Hazelcast binaries for version ")
                .append(version)
                .append(artifact.enterprise ? " EE " : " ")
                .append("will be downloaded from a remote repository. You can speed up the compatibility tests by "
                        + "installing the missing artifacts in your local maven repository so they don't have to be "
                        + "downloaded each time:\n $ mvn dependency:get -Dartifact=com.hazelcast:");
        sb.append(artifact.mavenProject)
                .append(":")
                .append(version)
                .append(artifact.test ? ":jar:tests" : "")
                .append(artifact.enterprise ? " -DremoteRepositories=https://repository.hazelcast.com/release" : "");
        LOGGER.warning(sb.toString());
    }
}
