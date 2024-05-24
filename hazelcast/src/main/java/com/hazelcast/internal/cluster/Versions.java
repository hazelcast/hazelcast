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

import java.lang.reflect.Field;
import java.text.MessageFormat;
import java.util.Objects;

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

    @Nonnull
    public static final Version PREVIOUS_CLUSTER_VERSION;
    @Nonnull
    public static final Version CURRENT_CLUSTER_VERSION;

    static {
        // Dynamically set PREVIOUS_CLUSTER_VERSION & CURRENT_CLUSTER_VERSION by reflection

        Version buildPropertiesVersion = Version.of(GeneratedBuildProperties.VERSION);

        // Find an equivalent version in the existing constant pool
        Version currentClusterVersionConstant = null;

        // The previous version is assumed to be the declared version lexicographically before CURRENT_CLUSTER_VERSION
        Version previousHighest = null;

        for (Field field : Versions.class.getFields()) {
            if (field.getType()
                    .equals(Version.class)) {
                try {
                    Version version = (Version) field.get(null);

                    if (version != null) {
                        int versionCompareToBuildPropertiesVersion = version.compareTo(buildPropertiesVersion);

                        if (versionCompareToBuildPropertiesVersion == 0) {
                            currentClusterVersionConstant = version;
                        } else if (versionCompareToBuildPropertiesVersion < 0
                                && (previousHighest == null || version.compareTo(previousHighest) > 0)) {
                            previousHighest = version;
                        }
                    }
                } catch (ReflectiveOperationException e) {
                    throw new ExceptionInInitializerError(e);
                }
            }
        }

        CURRENT_CLUSTER_VERSION = Objects.requireNonNull(currentClusterVersionConstant,
                () -> MessageFormat.format("Failed to find matching constant for version {0}", buildPropertiesVersion));

        PREVIOUS_CLUSTER_VERSION = Objects.requireNonNull(previousHighest,
                () -> MessageFormat.format("Failed to find version preceding {0}", CURRENT_CLUSTER_VERSION));
    }

    private Versions() {
    }
}
