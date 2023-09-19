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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.Preconditions.checkState;
import static com.hazelcast.test.JenkinsDetector.isOnJenkins;
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

    static {
        LOCAL_M2_REPOSITORY_PREFIX = System.getProperty("user.home") + separator + ".m2" + separator + "repository";
    }

    public static Map<Artifact, File> locateVersion(String version, boolean enterprise) {
        Map<Artifact, File> files = new HashMap<>();
        files.put(Artifact.OS_JAR, locateArtifact(Artifact.OS_JAR, version));
        files.put(Artifact.OS_TEST_JAR, locateArtifact(Artifact.OS_TEST_JAR, version));
        if (Version.of(version).isGreaterOrEqual(Versions.V5_0)) {
            files.put(Artifact.SQL_JAR, locateArtifact(Artifact.SQL_JAR, version));
        }
        if (enterprise) {
            files.put(Artifact.EE_JAR, locateArtifact(Artifact.EE_JAR, version));
            files.put(Artifact.EE_TEST_JAR, locateArtifact(Artifact.EE_TEST_JAR, version));
        }
        return files;
    }

    private static File locateArtifact(Artifact artifact, String version) {
        String path = format(artifact.path, version);
        File localCopy = new File(LOCAL_M2_REPOSITORY_PREFIX + path);
        if (!localCopy.exists()) {
            downloadArtifact(artifact, version);
        }
        return localCopy;
    }

    private static void downloadArtifact(Artifact artifact, String version) {
        logWarningForArtifactDownload(artifact, version);
        ProcessBuilder builder = new ProcessBuilder(buildMavenCommand(artifact, version).split(" "))
                .inheritIO();
        try {
            Process process = builder.start();
            boolean successful = process.waitFor(120, TimeUnit.SECONDS);
            checkState(successful, "Maven dependency:get timed out");
            checkState(process.exitValue() == 0, "Maven dependency:get failed");
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException("Problem in invoking Maven dependency:get " + artifact + ":" + version , e);
        }
    }

    private static void logWarningForArtifactDownload(Artifact artifact, String version) {
        if (isOnJenkins()) {
            return;
        }
        LOGGER.warning("Hazelcast binaries for version " + version
                + (artifact.enterprise ? " EE " : " ")
                + "will be downloaded from a remote repository. You can speed up the compatibility tests by "
                + "installing the missing artifacts in your local maven repository so they don't have to be "
                + "downloaded each time:\n $ " + buildMavenCommand(artifact, version));
    }

    private static String buildMavenCommand(Artifact artifact, String version) {
        return "mvn dependency:get -Dartifact=com.hazelcast:"
                + artifact.mavenProject + ":" + version
                + (artifact.test ? ":jar:tests" : "")
                + (artifact.enterprise ? " -DremoteRepositories=https://repository.hazelcast.com/release" : "");
    }
}

