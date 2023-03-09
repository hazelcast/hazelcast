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

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.internal.util.Sha256Util;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.impl.submitjob.clientside.SubmitJobTargetMemberFinder;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.UUID;

/**
 * Holds calculated parameters required for job upload
 */
public class JobUploadCall {

    private UUID sessionId;

    private UUID memberUuid;

    private String fileNameWithoutExtension;

    private String sha256HexOfJar;

    private int partSize;

    private int totalParts;

    public byte[] allocatePartBuffer() {
        return new byte[partSize];
    }

    public byte[] getDataToSend(byte[] partBuffer, int bytesRead) {
        byte[] dataToSend;
        if (bytesRead != partBuffer.length) {
            // If read less data, resize the array
            dataToSend = Arrays.copyOf(partBuffer, bytesRead);
        } else {
            // Otherwise use the full array
            dataToSend = partBuffer;
        }
        return dataToSend;
    }

    public void initializeJobUploadCall(HazelcastClientInstanceImpl client, Path jarPath)
            throws IOException, NoSuchAlgorithmException {
        // Create new session id
        this.sessionId = UuidUtil.newSecureUUID();

        // Get file name
        this.fileNameWithoutExtension = findFileNameWithoutExtension(jarPath);

        // Calculate digest for jar
        this.sha256HexOfJar = Sha256Util.calculateSha256Hex(jarPath);

        // Read jar's size
        long jarSize = Files.size(jarPath);

        // Calculate the part buffer size and the total parts for job upload
        SubmitJobPartCalculator calculator = new SubmitJobPartCalculator();
        HazelcastProperties hazelcastProperties = client.getProperties();
        this.partSize = calculator.calculatePartBufferSize(hazelcastProperties, jarSize);
        this.totalParts = calculator.calculateTotalParts(jarSize, partSize);

        // Find the destination member
        SubmitJobTargetMemberFinder submitJobTargetMemberFinder = new SubmitJobTargetMemberFinder();
        this.memberUuid = submitJobTargetMemberFinder.getRandomMemberId(client);
    }

    String findFileNameWithoutExtension(Path jarPath) {
        String fileName = jarPath.getFileName().toString();
        // Exclude the extension from filename
        int endIndex = fileName.lastIndexOf('.');
        if (endIndex != -1) {
            fileName = fileName.substring(0, endIndex);
        }
        return fileName;
    }

    public UUID getSessionId() {
        return sessionId;
    }

    public UUID getMemberUuid() {
        return memberUuid;
    }

    public String getFileNameWithoutExtension() {
        return fileNameWithoutExtension;
    }

    public void setFileNameWithoutExtension(String fileNameWithoutExtension) {
        this.fileNameWithoutExtension = fileNameWithoutExtension;
    }

    public String getSha256HexOfJar() {
        return sha256HexOfJar;
    }


    public void setSha256HexOfJar(String sha256HexOfJar) {
        this.sha256HexOfJar = sha256HexOfJar;
    }

    public int getTotalParts() {
        return totalParts;
    }
}
