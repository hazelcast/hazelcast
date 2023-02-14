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

import com.hazelcast.spi.properties.HazelcastProperties;

import static com.hazelcast.client.properties.ClientProperty.JOB_UPLOAD_PART_SIZE;

public class SubmitJobPartCalculator {

    /**
     * Calculate the part buffer size from properties if defined, otherwise use default value
     */
    int calculatePartBufferSize(HazelcastProperties hazelcastProperties) {
        return hazelcastProperties.getInteger(JOB_UPLOAD_PART_SIZE);
    }

    /**
     * Calculate the total parts required for the job upload
     */
    int calculateTotalParts(long jarSize, int partSize) {
        if (jarSize == partSize) {
            return 1;
        }
        return (int) Math.ceil(jarSize / (double) partSize);
    }
}
