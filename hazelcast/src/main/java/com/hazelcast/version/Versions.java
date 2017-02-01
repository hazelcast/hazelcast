/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.version;

import com.hazelcast.nio.Version;

/**
 * Version & ClusterVersion constants
 */
public final class Versions {

    /**
     * Version 3.8
     */
    public static final ClusterVersion CLUSTER_VERSION_3_8 = ClusterVersion.of("3.8");

    /**
     * Version 3.9
     */
    public static final ClusterVersion CLUSTER_VERSION_3_9 = ClusterVersion.of("3.8");

    /**
     * Version 8
     */
    public static final Version VERSION_8 = Version.of(8);

    /**
     * Version 9
     */
    public static final Version VERSION_9 = Version.of(9);

    private Versions() {
    }

}
