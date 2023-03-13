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

package com.hazelcast.jet.impl.submitjob.clientside.upload;

import com.hazelcast.spi.properties.HazelcastProperties;

import static com.hazelcast.client.properties.ClientProperty.JOB_UPLOAD_PART_SIZE;

public class SubmitJobPartCalculator {

    /**
     * Calculate the part buffer size finding minimum of <br/>
     * 1. size from the properties if defined, or the default value <br/>
     * 2. jarSize
     */
    int calculatePartBufferSize(HazelcastProperties hazelcastProperties, long jarSize) {
        int partBufferSize = hazelcastProperties.getInteger(JOB_UPLOAD_PART_SIZE);

        // If jar size is smaller, then use it
        if (jarSize < partBufferSize) {
            partBufferSize = (int) jarSize;
        }
        return partBufferSize;
    }

    /**
     * Calculate the total parts required for the job upload
     */
    int calculateTotalParts(long jarSize, long partSize) {
        long mod = jarSize % partSize;
        int remainder = (mod == 0L) ? 0 : 1;
        int result = (int) (jarSize / partSize);
        return result + remainder;
    }
}
