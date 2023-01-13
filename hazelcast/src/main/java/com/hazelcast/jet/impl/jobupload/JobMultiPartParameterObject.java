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

package com.hazelcast.jet.impl.jobupload;

import java.util.UUID;

/**
 * The wrapper for all part parameters to run an uploaded jar as Jet job
 */
public class JobMultiPartParameterObject {

    private UUID sessionId;
    private int currentPartNumber;
    private int totalPartNumber;
    private byte[] partData;
    private int partSize;

    public JobMultiPartParameterObject(UUID sessionId, int currentPartNumber, int totalPartNumber, byte[] partData,
                                       int partSize) {
        this.sessionId = sessionId;
        this.currentPartNumber = currentPartNumber;
        this.totalPartNumber = totalPartNumber;
        this.partData = partData;
        this.partSize = partSize;
    }

    public UUID getSessionId() {
        return sessionId;
    }

    public int getCurrentPartNumber() {
        return currentPartNumber;
    }

    public int getTotalPartNumber() {
        return totalPartNumber;
    }

    public byte[] getPartData() {
        return partData;
    }

    public int getPartSize() {
        return partSize;
    }
}
