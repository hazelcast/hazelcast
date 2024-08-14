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

package com.hazelcast.internal.cluster;

import com.hazelcast.instance.GeneratedBuildProperties;
import com.hazelcast.version.Version;

import javax.annotation.Nonnull;

public final class Versions {

    /**
     * Represents cluster version 4.0
     */
    public static final Version V4_0 = Version.of(4, 0);

    /**
     * Cluster version 4.1
     */
    public static final Version V4_1 = Version.of(4, 1);

    /**
     * Cluster version 4.2
     */
    public static final Version V4_2 = Version.of(4, 2);

    /**
     * Cluster version 5.0
     */
    public static final Version V5_0 = Version.of(5, 0);

    /**
     * Cluster version 5.1
     */
    public static final Version V5_1 = Version.of(5, 1);

    /**
     * Cluster version 5.2
     */
    public static final Version V5_2 = Version.of(5, 2);

    /**
     * Cluster version 5.3
     */
    public static final Version V5_3 = Version.of(5, 3);

    /**
     * Cluster version 5.4
     */
    public static final Version V5_4 = Version.of(5, 4);

    /**
     * Cluster version 5.5
     */
    public static final Version V5_5 = Version.of(5, 5);

    /**
     * Cluster version 6.0
     */
    public static final Version V6_0 = Version.of(6, 0);

    @Nonnull
    public static final Version CURRENT_CLUSTER_VERSION = Version.of(GeneratedBuildProperties.VERSION);

    private Versions() {
    }
}
