/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class SubmitJobPartCalculatorTest {

    @Test
    public void calculatePartBufferSize_when_validProperty() {
        SubmitJobPartCalculator submitJobPartCalculator = new SubmitJobPartCalculator();

        Properties properties = new Properties();
        properties.setProperty(ClientProperty.JOB_UPLOAD_PART_SIZE.getName(), "1000");

        HazelcastProperties hazelcastProperties = new HazelcastProperties(properties);

        int partSize = submitJobPartCalculator.calculatePartBufferSize(hazelcastProperties);
        assertEquals(1_000, partSize);
    }

    @Test
    public void calculatePartBufferSize_when_invalidProperty() {
        SubmitJobPartCalculator submitJobPartCalculator = new SubmitJobPartCalculator();

        Properties properties = new Properties();
        properties.setProperty(ClientProperty.JOB_UPLOAD_PART_SIZE.getName(), "E");

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(ClientProperty.JOB_UPLOAD_PART_SIZE.getName(), "E");

        HazelcastProperties hazelcastProperties = new HazelcastProperties(properties);

        assertThrows(NumberFormatException.class,
                () -> submitJobPartCalculator.calculatePartBufferSize(hazelcastProperties));

    }

    @Test
    public void calculateTotalParts() {
        long jarSize = 10_000_000;
        int partSize = 10_000_000;

        SubmitJobPartCalculator submitJobPartCalculator = new SubmitJobPartCalculator();

        int totalParts = submitJobPartCalculator.calculateTotalParts(jarSize, partSize);
        assertEquals(1, totalParts);
    }
}
