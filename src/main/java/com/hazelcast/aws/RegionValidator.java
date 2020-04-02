/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.hazelcast.aws;

import com.hazelcast.config.InvalidConfigurationException;

import java.util.regex.Pattern;

/**
 * Helper class used to validate AWS Region.
 */
final class RegionValidator {
    private static final Pattern AWS_REGION_PATTERN =
        Pattern.compile("\\w{2}(-gov-|-)(north|northeast|east|southeast|south|southwest|west|northwest|central)-\\d(?!.+)");

    private RegionValidator() {
    }

    static void validateRegion(String region) {
        if (!AWS_REGION_PATTERN.matcher(region).matches()) {
            String message = String.format("The provided region %s is not a valid AWS region.", region);
            throw new InvalidConfigurationException(message);
        }
    }
}
