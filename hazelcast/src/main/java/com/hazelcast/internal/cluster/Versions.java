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

package com.hazelcast.internal.cluster;

import com.hazelcast.version.Version;

public final class Versions {

    /**
     * Represents cluster version 3.8
     */
    public static final Version V3_8 = Version.of(3, 8);

    /**
     * Represents cluster version 3.9
     */
    public static final Version V3_9 = Version.of(3, 9);

    /**
     * Represents cluster version 3.10
     */
    public static final Version V3_10 = Version.of(3, 10);

    /**
     * Represents cluster version 3.11
     */
    public static final Version V3_11 = Version.of(3, 11);

    /**
     * Represents cluster version 3.12
     */
    public static final Version V3_12 = Version.of(3, 12);

    /**
     * Represents cluster version 4.0
     */
    public static final Version V4_0 = Version.of(4, 0);

    public static final Version PREVIOUS_CLUSTER_VERSION = V3_12;
    public static final Version CURRENT_CLUSTER_VERSION = V4_0;

    private Versions() {
    }
}
