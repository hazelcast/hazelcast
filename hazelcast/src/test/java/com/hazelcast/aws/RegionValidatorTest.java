/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.InvalidConfigurationException;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

import static com.hazelcast.test.HazelcastTestSupport.assertThrows;
import static org.junit.Assert.assertEquals;

public class RegionValidatorTest {

    private static final String[] VALID_AWS_REGIONS_MAY_2025 = new String[] {
            "af-south-1", "ap-east-1", "ap-northeast-1", "ap-northeast-2", "ap-northeast-3", "ap-south-1", "ap-south-2",
            "ap-southeast-1", "ap-southeast-2", "ap-southeast-3", "ap-southeast-4", "ap-southeast-5", "ap-southeast-7",
            "aws-cn-global", "aws-global", "aws-iso-b-global", "aws-iso-global", "aws-us-gov-global", "ca-central-1",
            "ca-west-1", "cn-north-1", "cn-northwest-1", "eu-central-1", "eu-central-2", "eu-isoe-west-1", "eu-north-1",
            "eu-south-1", "eu-south-2", "eu-west-1", "eu-west-2", "eu-west-3", "eusc-de-east-1", "il-central-1",
            "me-central-1", "me-south-1", "mx-central-1", "sa-east-1", "us-east-1", "us-east-2", "us-gov-east-1",
            "us-gov-west-1", "us-iso-east-1", "us-iso-west-1", "us-isob-east-1", "us-isof-east-1", "us-isof-south-1",
            "us-west-1", "us-west-2"
    };

    @Test
    public void validateValidRegion() {
        for (String region : VALID_AWS_REGIONS_MAY_2025) {
            RegionValidator.validateRegion(region);
        }
    }

    @Test
    public void validateInvalidRegion() {
        // given
        String region = "us-wrong-1";
        String expectedMessage = String.format("The provided region %s is not a valid AWS region.", region);

        //when
        ThrowingRunnable validateRegion = () -> RegionValidator.validateRegion(region);

        //then
        InvalidConfigurationException thrownEx = assertThrows(InvalidConfigurationException.class, validateRegion);
        assertEquals(expectedMessage, thrownEx.getMessage());
    }

    @Test
    public void validateInvalidGovRegion() {
        // given
        String region = "us-gov-wrong-1";
        String expectedMessage = String.format("The provided region %s is not a valid AWS region.", region);

        // when
        ThrowingRunnable validateRegion = () -> RegionValidator.validateRegion(region);

        //then
        InvalidConfigurationException thrownEx = assertThrows(InvalidConfigurationException.class, validateRegion);
        assertEquals(expectedMessage, thrownEx.getMessage());
    }

    @Test
    public void validateNullRegion() {
        // given
        String expectedMessage = "The provided region is null.";

        // when
        ThrowingRunnable validateRegion = () -> RegionValidator.validateRegion(null);

        //then
        InvalidConfigurationException thrownEx = assertThrows(InvalidConfigurationException.class, validateRegion);
        assertEquals(expectedMessage, thrownEx.getMessage());
    }
}
