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

package com.hazelcast.jet.impl.submitjob.memberside;

import java.util.UUID;

/**
 * Used by the member side as the wrapper for all part parameters to run an uploaded jar as Jet job
 */
public class JobMultiPartParameterObject {

    private UUID sessionId;

    private int currentPartNumber;

    private int totalPartNumber;

    private byte[] partData;

    private int partSize;

    private String sha256Hex;

    public UUID getSessionId() {
        return sessionId;
    }

    public void setSessionId(UUID sessionId) {
        this.sessionId = sessionId;
    }

    public int getCurrentPartNumber() {
        return currentPartNumber;
    }

    public void setCurrentPartNumber(int currentPartNumber) {
        this.currentPartNumber = currentPartNumber;
    }

    public int getTotalPartNumber() {
        return totalPartNumber;
    }

    public void setTotalPartNumber(int totalPartNumber) {
        this.totalPartNumber = totalPartNumber;
    }

    public byte[] getPartData() {
        return partData;
    }

    public void setPartData(byte[] partData) {
        this.partData = partData;
    }

    public int getPartSize() {
        return partSize;
    }

    public void setPartSize(int partSize) {
        this.partSize = partSize;
    }

    public String getSha256Hex() {
        return sha256Hex;
    }

    public void setSha256Hex(String sha256Hex) {
        this.sha256Hex = sha256Hex;
    }
}
