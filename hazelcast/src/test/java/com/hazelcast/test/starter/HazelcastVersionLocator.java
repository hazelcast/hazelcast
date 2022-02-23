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

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.nio.IOUtil.drainTo;
import static com.hazelcast.test.JenkinsDetector.isOnJenkins;
import static com.hazelcast.test.starter.HazelcastStarterUtils.rethrowGuardianException;
import static java.io.File.separator;
import static java.lang.String.format;

public class HazelcastVersionLocator {

    // artifact indexes
    public static final int HAZELCAST_JAR_INDEX = 0;
    public static final int HAZELCAST_TESTS_JAR_INDEX = 1;
    public static final int HAZELCAST_EE_JAR_INDEX = 2;
    public static final int HAZELCAST_EE_TESTS_JAR_INDEX = 3;

    private static final ILogger LOGGER = Logger.getLogger(HazelcastVersionLocator.class);

    private static final String LOCAL_M2_REPOSITORY_PREFIX;
    private static final String MAVEN_CENTRAL_PREFIX;
    private static final String HAZELCAST_REPOSITORY_PREFIX;

    private static final String OS_PATH = "/com/hazelcast/hazelcast/%1$s/hazelcast-%1$s.jar";
    private static final String OS_TESTS_PATH = "/com/hazelcast/hazelcast/%1$s/hazelcast-%1$s-tests.jar";
    private static final String EE_PATH = "/com/hazelcast/hazelcast-enterprise/%1$s/hazelcast-enterprise-%1$s.jar";
    private static final String EE_TESTS_PATH = "/com/hazelcast/hazelcast-enterprise/%1$s/hazelcast-enterprise-%1$s-tests.jar";

    static {
        LOCAL_M2_REPOSITORY_PREFIX = System.getProperty("user.home") + separator + ".m2" + separator + "repository";
        MAVEN_CENTRAL_PREFIX = "https://repo1.maven.org/maven2";
        HAZELCAST_REPOSITORY_PREFIX = "https://repository.hazelcast.com/release";
    }

    public static File[] locateVersion(String version, File target, boolean enterprise) {
        File[] files = new File[enterprise ? 4 : 2];
        files[HAZELCAST_JAR_INDEX] = locateMainArtifact(version, target, false);
        files[HAZELCAST_TESTS_JAR_INDEX] = locateTestsArtifact(version, target, false);
        if (enterprise) {
            files[HAZELCAST_EE_JAR_INDEX] = locateMainArtifact(version, target, true);
            files[HAZELCAST_EE_TESTS_JAR_INDEX] = locateTestsArtifact(version, target, true);
        }
        return files;
    }

    // attempts to locate hazelcast artifact in local maven repository, then downloads
    private static File locateMainArtifact(String version, File target, boolean enterprise) {
        File artifact = new File(LOCAL_M2_REPOSITORY_PREFIX + constructPath(version, enterprise));
        if (artifact.exists()) {
            return artifact;
        } else {
            return downloadMainArtifact(version, target, enterprise);
        }
    }

    // attempts to locate tests artifact in local maven repository, then downloads
    private static File locateTestsArtifact(String version, File target, boolean enterprise) {
        File artifact = new File(LOCAL_M2_REPOSITORY_PREFIX + constructPathForTests(version, enterprise));
        if (artifact.exists()) {
            return artifact;
        } else {
            return downloadTestsArtifact(version, target, enterprise);
        }
    }

    private static File downloadMainArtifact(String version, File target, boolean enterprise) {
        String url = constructUrl(version, enterprise);
        String filename = extractFilenameFromUrl(url);
        logWarningForArtifactDownload(version, false, enterprise);
        return downloadFile(url, target, filename);
    }

    private static File downloadTestsArtifact(String version, File target, boolean enterprise) {
        String url = constructUrlForTests(version, enterprise);
        String filename = extractFilenameFromUrl(url);
        logWarningForArtifactDownload(version, true, enterprise);
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

    private static String constructUrl(String version, boolean enterprise) {
        return (enterprise ? HAZELCAST_REPOSITORY_PREFIX : MAVEN_CENTRAL_PREFIX)
                + constructPath(version, enterprise);
    }

    private static String constructUrlForTests(String version, boolean enterprise) {
        return (enterprise ? HAZELCAST_REPOSITORY_PREFIX : MAVEN_CENTRAL_PREFIX)
                + constructPathForTests(version, enterprise);
    }

    private static String constructPath(String version, boolean enterprise) {
        return format(enterprise ? EE_PATH : OS_PATH, version);
    }

    private static String constructPathForTests(String version, boolean enterprise) {
        return format(enterprise ? EE_TESTS_PATH : OS_TESTS_PATH, version);
    }

    private static void logWarningForArtifactDownload(String version, boolean tests, boolean enterprise) {
        if (isOnJenkins()) {
            return;
        }

        StringBuilder sb = new StringBuilder();
        sb.append("Hazelcast binaries for version ").append(version).append(enterprise ? " EE " : " ")
                .append("will be downloaded from a remote repository. You can speed up the compatibility tests by "
                        + "installing the missing artifacts in your local maven repository so they don't have to be "
                        + "downloaded each time:\n $ mvn dependency:get -Dartifact=com.hazelcast:");
        sb.append(enterprise ? "hazelcast-enterprise:" : "hazelcast:")
          .append(version)
          .append(tests ? ":jar:tests" : "")
          .append(enterprise ? " -DremoteRepositories=https://repository.hazelcast.com/release" : "");
        LOGGER.warning(sb.toString());
    }
}
