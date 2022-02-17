/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import java.util.Map;
import java.util.Optional;

/**
 * Responsible for fetching discovery information from AWS APIs.
 */
interface AwsClient {
    Map<String, String> getAddresses();

    String getAvailabilityZone();

    /**
     * Returns the placement group name of the service if specified.
     *
     * @see <a href="https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/placement-groups.html">Placement Groups</a>
     */
    default Optional<String> getPlacementGroup() {
        return Optional.empty();
    }

    /**
     * Returns the partition number of the service if it belongs to a partition placement group.
     *
     * @see <a href="https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/placement-groups.html#placement-groups-partition">
     *     Partition Placement Groups</a>
     */
    default Optional<String> getPlacementPartitionNumber() {
        return Optional.empty();
    }
}
