/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import static java.lang.System.getenv;

/**
 * Attempt to detect whether code is a test running on Jenkins.
 */
public final class JenkinsDetector {

    private JenkinsDetector() {
    }

    public static boolean isOnJenkins() {
        return getenv("JENKINS_URL") != null
                && getenv("BUILD_NUMBER") != null
                && getenv("NODE_NAME") != null;
    }
}
