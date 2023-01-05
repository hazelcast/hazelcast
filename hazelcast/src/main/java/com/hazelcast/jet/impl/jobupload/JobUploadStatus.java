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

package com.hazelcast.jet.impl.jobupload;

import com.hazelcast.jet.JetException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

import static com.hazelcast.internal.util.Sha256Util.calculateSha256Hex;

public class JobUploadStatus {

    private static final ILogger LOGGER = Logger.getLogger(JobUploadStatus.class);

    private static final long EXPIRATION_MINUTES = 1;

    protected Instant lastUpdatedTime;
    private final Clock clock;

    private int currentPart;
    private int totalPart;

    private final JobMetaDataParameterObject jobMetaDataParameterObject;


    public JobUploadStatus(JobMetaDataParameterObject parameterObject) {
        this(parameterObject, Clock.systemUTC());
    }

    public JobUploadStatus(JobMetaDataParameterObject parameterObject, Clock clock) {
        this.jobMetaDataParameterObject = parameterObject;
        this.clock = clock;
        changeLastUpdatedTime();
    }

    public boolean isExpired() {
        Instant now = clock.instant();
        Duration between = Duration.between(lastUpdatedTime, now);
        // Check if total number of minutes is bigger
        long minutes = between.toMinutes();
        return minutes >= EXPIRATION_MINUTES;
    }

    // Called when this object is removed from JobUploadStore
    public void onRemove() {
        try {
            if (jobMetaDataParameterObject.getJarPath() != null) {
                Files.delete(jobMetaDataParameterObject.getJarPath());
            }
        } catch (IOException exception) {
            LOGGER.severe("Error while deleting file : " + jobMetaDataParameterObject.getJarPath(), exception);
        }
    }

    public JobMetaDataParameterObject processJobMultipart(JobMultiPartParameterObject parameterObject)
            throws IOException, NoSuchAlgorithmException {

        validateReceivedParameters(parameterObject);

        validateReceivedPartNumbersAreExpected(parameterObject);

        // Parts numbers are good. Save them
        currentPart = parameterObject.getCurrentPartNumber();
        totalPart = parameterObject.getTotalPartNumber();

        // If the first part
        if (currentPart == 1) {
            createNewTemporaryFile();
        }

        Path jarPath = jobMetaDataParameterObject.getJarPath();

        // Append data to file
        try (FileOutputStream outputStream = new FileOutputStream(jarPath.toFile(), true)) {
            outputStream.write(parameterObject.getPartData(), 0, parameterObject.getPartSize());
        }

        String message = String.format("Session : %s total file size %d", parameterObject.getSessionId(), Files.size(jarPath));
        LOGGER.info(message);

        changeLastUpdatedTime();

        JobMetaDataParameterObject result = null;
        // If parts are complete
        if (currentPart == totalPart) {

            validateChecksum();

            result = jobMetaDataParameterObject;
        }
        return result;
    }

    private void createNewTemporaryFile() throws IOException {
        // Create a new temporary file
        Path jarPath = Files.createTempFile("runjob", ".jar"); //NOSONAR

        // Make it accessible only by the owner
        File jarFile = jarPath.toFile();
        boolean success = jarFile.setReadable(true, true);
        if (!success) {
            LOGGER.info("setReadable failed on " + jarFile);
        }
        success = jarFile.setWritable(true, true);
        if (!success) {
            LOGGER.info("setWritable failed on " + jarFile);
        }
        success = jarFile.setExecutable(true, true);
        if (!success) {
            LOGGER.info("setExecutable failed on " + jarFile);
        }
        jobMetaDataParameterObject.setJarPath(jarPath);
    }

    private static void validateReceivedParameters(JobMultiPartParameterObject parameterObject) {
        int receivedCurrentPart = parameterObject.getCurrentPartNumber();
        int receivedTotalPart = parameterObject.getTotalPartNumber();

        //Ensure positive number
        if (receivedCurrentPart <= 0) {
            String errorMessage = String.format("receivedPart : %d is incorrect", receivedCurrentPart);
            throw new JetException(errorMessage);
        }

        if (receivedTotalPart <= 0) {
            String errorMessage = String.format("receivedTotalPart : %d is incorrect", receivedTotalPart);
            throw new JetException(errorMessage);
        }

        //Ensure relative order
        if (receivedCurrentPart > receivedTotalPart) {
            String errorMessage = String.format("receivedPart : %d is bigger than receivedTotalPart : %d ",
                    receivedCurrentPart, receivedTotalPart);
            throw new JetException(errorMessage);
        }

        //Ensure part data is not null
        byte[] partData = parameterObject.getPartData();
        if (partData == null) {
            String errorMessage = "receivedPartData is null";
            throw new JetException(errorMessage);
        }

        //Ensure part data size is positive
        if (partData.length == 0) {
            String errorMessage = "receivedPartData size is 0";
            throw new JetException(errorMessage);
        }

        //Ensure part size is positive
        int partSize = parameterObject.getPartSize();
        if (partSize == 0) {
            String errorMessage = "receivedPartSize is 0";
            throw new JetException(errorMessage);
        }

        //Ensure part size is valid
        if (partSize > partData.length) {
            String errorMessage = String.format("receivedPartSize: %d is bigger than receivedPartLength : %d ", partSize,
                    partData.length);
            throw new JetException(errorMessage);
        }
    }

    private void validateReceivedPartNumbersAreExpected(JobMultiPartParameterObject parameterObject) {
        int receivedCurrentPart = parameterObject.getCurrentPartNumber();
        int receivedTotalPart = parameterObject.getTotalPartNumber();

        if (currentPart >= receivedCurrentPart) {
            String errorMessage = String.format("Received an old order part. currentPart : %d receivedPart : %d",
                    currentPart, receivedCurrentPart);
            throw new JetException(errorMessage);
        }

        if (currentPart + 1 != receivedCurrentPart) {
            String errorMessage = String.format("Received an out of order part. currentPart : %d receivedPart : %d",
                    currentPart, receivedCurrentPart);
            throw new JetException(errorMessage);
        }

        if (totalPart != 0 && totalPart != receivedTotalPart) {
            String errorMessage = String.format("Received a different totalPart. totalPart : %d receivedTotalPart : %d",
                    totalPart, receivedTotalPart);
            throw new JetException(errorMessage);
        }
    }

    private void validateChecksum() throws IOException, NoSuchAlgorithmException {
        String calculatedSha256Hex = calculateSha256Hex(jobMetaDataParameterObject.getJarPath());
        String receivedSha256Hex = jobMetaDataParameterObject.getSha256Hex();
        if (!calculatedSha256Hex.equals(receivedSha256Hex)) {
            String errorMessage = String.format("Checksum is different!. Calculated SHA256 : %s. Received SHA256 : %s",
                    calculatedSha256Hex, receivedSha256Hex);
            throw new JetException(errorMessage);
        }
    }

    protected void changeLastUpdatedTime() {
        this.lastUpdatedTime = clock.instant();
    }
}
