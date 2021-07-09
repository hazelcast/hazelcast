/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.aws;

import static com.hazelcast.aws.StringUtils.isNotEmpty;

/**
 * This class is introduced to lookup system parameters.
 */
class Environment {
    private static final String AWS_REGION_ON_ECS = System.getenv("AWS_REGION");
    private static final boolean IS_RUNNING_ON_ECS = isRunningOnEcsEnvironment();

    String getAwsRegionOnEcs() {
        return AWS_REGION_ON_ECS;
    }

    boolean isRunningOnEcs() {
        return IS_RUNNING_ON_ECS;
    }

    private static boolean isRunningOnEcsEnvironment() {
        String execEnv = System.getenv("AWS_EXECUTION_ENV");
        return isNotEmpty(execEnv) && execEnv.contains("ECS");
    }
}
