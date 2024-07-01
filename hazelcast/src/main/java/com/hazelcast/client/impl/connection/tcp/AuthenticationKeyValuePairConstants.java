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

package com.hazelcast.client.impl.connection.tcp;

import com.hazelcast.client.UnsupportedClusterVersionException;
import com.hazelcast.client.UnsupportedRoutingModeException;
import com.hazelcast.client.config.SubsetRoutingConfig;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.version.Version;

import java.util.Map;

import static com.hazelcast.client.config.RoutingStrategy.PARTITION_GROUPS;

/**
 * Client uses the constants defined in this class to look up fields
 * in keyValuePairs which is sent from the server-side.
 *
 * @see KeyValuePairGenerator
 */
public final class AuthenticationKeyValuePairConstants {

    public static final String CLUSTER_VERSION = "clusterVersion";
    public static final String ROUTING_MODE_NOT_SUPPORTED_MESSAGE = String.format(
            "Subset routing strategy %s cannot be supported because the server has not sent "
                    + "the required information. Subset routing is an Enterprise feature in Hazelcast 5.5. "
                    + "Make sure your cluster has Hazelcast Enterprise JARs on its classpath.",
            PARTITION_GROUPS);
    public static final String SUBSET_MEMBER_GROUPS_INFO = "memberGroups";
    public static final String CP_LEADERS_INFO = "cp.leaders";
    private static final Version SUBSET_ROUTING_MINIMUM_SUPPORTED_CLUSTER_VERSION = Versions.V5_5;


    private AuthenticationKeyValuePairConstants() { }

    public static boolean checkRequiredFieldsForSubsetRoutingExist(SubsetRoutingConfig subsetRoutingConfig,
                                                                   Map<String, String> keyValuePairs) {
        if (!subsetRoutingConfig.isEnabled()) {
            return false;
        }

        if (subsetRoutingConfig.getRoutingStrategy() != PARTITION_GROUPS) {
            return false;
        }

        if (!keyValuePairs.containsKey(SUBSET_MEMBER_GROUPS_INFO)) {
            throw new UnsupportedRoutingModeException(ROUTING_MODE_NOT_SUPPORTED_MESSAGE);
        }

        return true;
    }

    public static void checkMinimumClusterVersionForSubsetRouting(Map<String, String> keyValuePairs) {
        if (!keyValuePairs.containsKey(CLUSTER_VERSION)
                || Version.of(keyValuePairs.get(CLUSTER_VERSION)).isUnknown()
                || Version.of(keyValuePairs.get(CLUSTER_VERSION)).isLessThan(SUBSET_ROUTING_MINIMUM_SUPPORTED_CLUSTER_VERSION)) {
            throw new UnsupportedClusterVersionException(ROUTING_MODE_NOT_SUPPORTED_MESSAGE);
        }
    }
}
