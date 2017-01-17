package com.hazelcast.test;

import static java.lang.System.getenv;

/**
 * Attempt to detect whether code is a test running on Jenkins.
 *
 */
public class JenkinsDetector {
    public static boolean isOnJenkins() {
        return getenv("JENKINS_URL") != null &&
               getenv("BUILD_NUMBER") != null &&
               getenv("NODE_NAME") != null;
    }
}
