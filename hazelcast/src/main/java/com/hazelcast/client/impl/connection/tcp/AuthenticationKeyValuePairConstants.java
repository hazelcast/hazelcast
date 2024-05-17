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

import com.hazelcast.client.config.RoutingStrategy;
import com.hazelcast.client.config.SubsetRoutingConfig;

import java.util.Map;

/**
 * Client uses the constants defined in this class to look up fields
 * in keyValuePairs which is sent from the server-side.
 *
 * @see KeyValuePairGenerator
 */
public final class AuthenticationKeyValuePairConstants {

    public static final String CLUSTER_VERSION = "clusterVersion";
    public static final String SUBSET_MEMBER_GROUPS_INFO = "memberGroups";

    private AuthenticationKeyValuePairConstants() { }

    public static boolean checkRequiredFieldsForSubsetRoutingExist(SubsetRoutingConfig subsetRoutingConfig,
                                                                   Map<String, String> keyValuePairs) {
        if (!subsetRoutingConfig.isEnabled()) {
            return false;
        }

        if (subsetRoutingConfig.getRoutingStrategy() != RoutingStrategy.PARTITION_GROUPS) {
            return false;
        }

        if (!keyValuePairs.containsKey(SUBSET_MEMBER_GROUPS_INFO)) {
            String msg = String.format("Subset routing strategy %s", RoutingStrategy.PARTITION_GROUPS
                    + " cannot be supported because the server has not sent the required information. "
                    + "Subset routing is an Enterprise feature. "
                    + "Make sure you have Hazelcast Enterprise JARs on your classpath!");
            throw new UnsupportedOperationException(msg);
        }

        return true;
    }
}
