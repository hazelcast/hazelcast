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

    private static final ILogger LOGGER = Logger.getLogger(HazelcastVersionLocator.class);

    private static final String LOCAL_M2_REPOSITORY_PREFIX;
    private static final String MAVEN_CENTRAL_PREFIX;
    private static final String HAZELCAST_REPOSITORY_PREFIX;

    private static final String MEMBER_PATH = "/com/hazelcast/hazelcast/%1$s/hazelcast-%1$s.jar";
    private static final String MEMBER_TESTS_PATH = "/com/hazelcast/hazelcast/%1$s/hazelcast-%1$s-tests.jar";
    private static final String EE_MEMBER_PATH = "/com/hazelcast/hazelcast-enterprise/%1$s/hazelcast-enterprise-%1$s.jar";
    private static final String EE_MEMBER_TESTS_PATH = "/com/hazelcast/hazelcast-enterprise/%1$s/hazelcast-enterprise-%1$s-tests.jar";
    private static final String CLIENT_PATH = "/com/hazelcast/hazelcast-client/%1$s/hazelcast-client-%1$s.jar";
    private static final String EE_CLIENT_PATH
            = "/com/hazelcast/hazelcast-enterprise-client/%1$s/hazelcast-enterprise-client-%1$s.jar";

    static {
        LOCAL_M2_REPOSITORY_PREFIX = System.getProperty("user.home") + separator + ".m2" + separator + "repository";
        MAVEN_CENTRAL_PREFIX = "https://repo1.maven.org/maven2";
        HAZELCAST_REPOSITORY_PREFIX = "https://repository.hazelcast.com/release";
    }

    public static File[] locateVersion(String version, File target, boolean enterprise) {
        File[] files = new File[enterprise ? 6 : 3];
        files[0] = locateMember(version, target, false);
        files[1] = locateMemberTests(version, target, false);
        files[2] = locateClient(version, target, false);
        if (enterprise) {
            files[3] = locateMember(version, target, true);
            files[4] = locateMemberTests(version, target, true);
            files[5] = locateClient(version, target, true);
        }
        return files;
    }

    // attempts to locate member artifact in local maven repository, then downloads
    private static File locateMember(String version, File target, boolean enterprise) {
        File artifact = new File(LOCAL_M2_REPOSITORY_PREFIX + constructPathForMember(version, enterprise));
        if (artifact.exists()) {
            return artifact;
        } else {
            return downloadMember(version, target, enterprise);
        }
    }

    // attempts to locate member tests artifact in local maven repository, then downloads
    private static File locateMemberTests(String version, File target, boolean enterprise) {
        File artifact = new File(LOCAL_M2_REPOSITORY_PREFIX + constructPathForMemberTests(version, enterprise));
        if (artifact.exists()) {
            return artifact;
        } else {
            return downloadMemberTests(version, target, enterprise);
        }
    }

    // first attempt to locate artifact in local maven repository, then download
    private static File locateClient(String version, File target, boolean enterprise) {
        File artifact = new File(LOCAL_M2_REPOSITORY_PREFIX + constructPathForClient(version, enterprise));
        if (artifact.exists()) {
            return artifact;
        } else {
            return downloadClient(version, target, enterprise);
        }
    }

    private static File downloadClient(String version, File target, boolean enterprise) {
        String url = constructUrlForClient(version, enterprise);
        String filename = extractFilenameFromUrl(url);
        logWarningForArtifactDownload(version, false, enterprise);
        return downloadFile(url, target, filename);
    }

    private static File downloadMember(String version, File target, boolean enterprise) {
        String url = constructUrlForMember(version, enterprise);
        String filename = extractFilenameFromUrl(url);
        logWarningForArtifactDownload(version, true, enterprise);
        return downloadFile(url, target, filename);
    }

    private static File downloadMemberTests(String version, File target, boolean enterprise) {
        String url = constructUrlForMemberTests(version, enterprise);
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

    private static String constructUrlForClient(String version, boolean enterprise) {
        return (enterprise ? HAZELCAST_REPOSITORY_PREFIX : MAVEN_CENTRAL_PREFIX)
                + constructPathForClient(version, enterprise);
    }

    private static String constructUrlForMember(String version, boolean enterprise) {
        return (enterprise ? HAZELCAST_REPOSITORY_PREFIX : MAVEN_CENTRAL_PREFIX)
                + constructPathForMember(version, enterprise);
    }

    private static String constructUrlForMemberTests(String version, boolean enterprise) {
        return (enterprise ? HAZELCAST_REPOSITORY_PREFIX : MAVEN_CENTRAL_PREFIX)
                + constructPathForMemberTests(version, enterprise);
    }

    private static String constructPathForClient(String version, boolean enterprise) {
        return format(enterprise ? EE_CLIENT_PATH : CLIENT_PATH, version);
    }

    private static String constructPathForMember(String version, boolean enterprise) {
        return format(enterprise ? EE_MEMBER_PATH : MEMBER_PATH, version);
    }

    private static String constructPathForMemberTests(String version, boolean enterprise) {
        return format(enterprise ? EE_MEMBER_TESTS_PATH : MEMBER_TESTS_PATH, version);
    }

    private static void logWarningForArtifactDownload(String version, boolean member, boolean enterprise) {
        if (isOnJenkins()) {
            return;
        }

        StringBuilder sb = new StringBuilder();
        sb.append("Hazelcast binaries for version ").append(version).append(enterprise ? " EE " : " ")
                .append("will be downloaded from a remote repository. You can speed up the compatibility tests by "
                        + "installing the missing artifacts in your local maven repository so they don't have to be "
                        + "downloaded each time:\n $ mvn dependency:get -Dartifact=com.hazelcast:");
        if (enterprise) {
            sb.append(member ? "hazelcast-enterprise:" : "hazelcast-enterprise-client:");
        } else {
            sb.append(member ? "hazelcast:" : "hazelcast-client:");
        }
        sb.append(version)
                .append(enterprise ? " -DremoteRepositories=https://repository.hazelcast.com/release" : "");
        LOGGER.warning(sb.toString());
    }
}
