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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Streams;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.net.URL;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RegionValidatorTest {

    private static Stream<String> getAWSRegions() throws IOException {
        URL source = new URL("https://raw.githubusercontent.com/boto/botocore/develop/botocore/data/endpoints.json");
        JsonNode tree = new ObjectMapper().readTree(source);
        JsonNode partitions = tree.get("partitions");
        Stream<JsonNode> regionArrays = partitions.valueStream().map(partition -> partition.get("regions"));
        Stream<String> regionNames = regionArrays.flatMap(region -> Streams.stream(region.fieldNames()));

        return regionNames;
    }

    /**
     * Because this has an external dependencies, it's not sensible to check it on a per-PR basis as it could start failing due
     * to unrelated changes
     */
    @SlowTest
    @ParameterizedTest
    @MethodSource("getAWSRegions")
    void getAWSRegions(String region) {
        assertDoesNotThrow(() -> RegionValidator.validateRegion(region));
    }

    private static Stream<Arguments> invalidRegions() {
        return Stream.of(
                Arguments.of(Named.of("Invalid Region", "us-wrong-1"), "The provided region %s is not a valid AWS region."),
                Arguments.of(Named.of("Invalid Gov Region", "us-gov-wrong-1"),
                        "The provided region %s is not a valid AWS region."),
                Arguments.of(Named.of("Null Region", null), "The provided region is null."));
    }

    @ParameterizedTest
    @MethodSource("invalidRegions")
    void validateInvalidRegion(String region, String expectedMessage) {
        // when
        Executable validateRegion = () -> RegionValidator.validateRegion(region);

        //then
        Exception thrownEx = assertThrows(InvalidConfigurationException.class, validateRegion);
        assertEquals(String.format(expectedMessage, region), thrownEx.getMessage());
    }
}
