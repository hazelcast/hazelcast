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

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;

public class JobUploadStatus {

    private static ILogger logger = Logger.getLogger(JobUploadStatus.class);

    private static final long EXPIRATION_MINUTES = 1;

    private Instant lastUpdatedTime = Instant.now();

    private int currentPart;
    private int totalPart;

    private final JobMetaDataParameterObject jobMetaDataParameterObject;

    public JobUploadStatus(JobMetaDataParameterObject parameterObject) {
        this.jobMetaDataParameterObject = parameterObject;
    }

    public boolean isExpired() {
        Instant now = Instant.now();
        Duration between = Duration.between(lastUpdatedTime, now);
        // Check if total number of minutes is bigger
        long minutes = between.toMinutes();
        return minutes >= EXPIRATION_MINUTES;
    }

    public void onRemove() {
        try {
            if (jobMetaDataParameterObject.getJarPath() != null) {
                Files.delete(jobMetaDataParameterObject.getJarPath());
            }
        } catch (IOException exception) {
            logger.severe("Error while deleting file : " + jobMetaDataParameterObject.getJarPath(), exception);
        }
    }

    public JobMetaDataParameterObject processJarData(JobMultiPartParameterObject parameterObject) throws IOException {

        ensureReceivedPartNumbersAreValid(parameterObject);

        ensureReceivedPartNumbersAreExpected(parameterObject);

        // Parts numbers are good. Save them
        currentPart = parameterObject.getCurrentPartNumber();
        totalPart = parameterObject.getTotalPartNumber();

        Path jarPath = jobMetaDataParameterObject.getJarPath();

        // If the first part
        if (currentPart == 1) {
            //Create a new temporary file
            jarPath = Files.createTempFile("runjob", ".jar");
            jobMetaDataParameterObject.setJarPath(jarPath);
        }

        // Append data to file
        try (FileOutputStream outputStream = new FileOutputStream(jarPath.toFile(), true)) {
            outputStream.write(parameterObject.getPartData(), 0, parameterObject.getPartSize());
        }

        String message = String.format("Session : %s total file size %d", parameterObject.getSessionId(), Files.size(jarPath));
        logger.info(message);

        changeLastUpdatedTime();

        JobMetaDataParameterObject result = null;
        //Return if parts are complete
        if (currentPart == totalPart) {
            result = jobMetaDataParameterObject;
        }
        return result;
    }

    private static void ensureReceivedPartNumbersAreValid(JobMultiPartParameterObject parameterObject) {
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
    }

    private void ensureReceivedPartNumbersAreExpected(JobMultiPartParameterObject parameterObject) {
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

    protected void changeLastUpdatedTime() {
        this.lastUpdatedTime = Instant.now();
    }
}
