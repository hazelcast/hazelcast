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

import com.hazelcast.internal.util.Sha256Util;
import com.hazelcast.jet.JetException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

import static com.hazelcast.internal.util.Sha256Util.calculateSha256Hex;

/**
 * Used by the member side to hold the details of a job that is being uploaded
 */
public class JobUploadStatus {

    // Indicates the elapsed duration for an entry to expire
    protected static final long EXPIRATION_MINUTES = 2;

    private static final ILogger LOGGER = Logger.getLogger(JobUploadStatus.class);

    // The instant when this object was updated
    protected Instant lastUpdatedTime;

    // Clock used to get Instant
    private final Clock clock;

    // Last processed value
    private int currentPart;

    // Last processed value
    private int totalPart;

    // Metadata about the upload
    private final JobMetaDataParameterObject jobMetaDataParameterObject;

    public JobUploadStatus(JobMetaDataParameterObject parameterObject) {
        this(parameterObject, Clock.systemUTC());
    }

    // Get an external Clock for easy unit testing
    protected JobUploadStatus(JobMetaDataParameterObject parameterObject, Clock clock) {
        this.jobMetaDataParameterObject = parameterObject;
        this.clock = clock;
        changeLastUpdatedTime();
    }

    public JobMetaDataParameterObject getJobMetaDataParameterObject() {
        return jobMetaDataParameterObject;
    }

    /**
     * Returns if this instance is considered expired or not
     *
     * @return true if expired, false otherwise
     */
    public boolean isExpired() {
        Instant now = clock.instant();
        Duration between = Duration.between(lastUpdatedTime, now);
        // Compare elapsed time to EXPIRATION_MINUTES
        long minutes = between.toMinutes();
        return minutes >= EXPIRATION_MINUTES;
    }

    /**
     * Called when this object is removed from JobUploadStore because of an exception or expiration
     */
    public void removeBadSession() {
        cleanup(jobMetaDataParameterObject);
    }

    /**
     * Perform cleaning of the JobMetaDataParameterObject
     */
    public static void cleanup(JobMetaDataParameterObject jobMetaDataParameterObject) {
        if (jobMetaDataParameterObject.isJarOnClient()) {
            Path jarPath = jobMetaDataParameterObject.getJarPath();
            if (jarPath != null) {
                try {
                    Files.delete(jarPath);
                } catch (IOException exception) {
                    LOGGER.severe("Could not delete the jar : " + jarPath, exception);
                }
            }
        }
    }

    /**
     * Process the part message for the job upload
     *
     * @param parameterObject specified message from client
     * @return JobMetaDataParameterObject if upload is complete
     * @throws IOException              in case of I/O error
     * @throws NoSuchAlgorithmException in case of message digest error
     * @throws JetException             in case of other errors
     */
    public JobMetaDataParameterObject processJobMultipart(JobMultiPartParameterObject parameterObject)
            throws IOException, NoSuchAlgorithmException {
        // Change the timestamp in the beginning to avoid expiration
        changeLastUpdatedTime();

        validateReceivedParameters(parameterObject);

        validateReceivedPartNumbersAreExpected(parameterObject);

        validatePartChecksum(parameterObject);

        // Parts numbers are good. Save them
        currentPart = parameterObject.getCurrentPartNumber();
        totalPart = parameterObject.getTotalPartNumber();

        Path jarPath = jobMetaDataParameterObject.getJarPath();

        // Append data to file
        try (FileOutputStream outputStream = new FileOutputStream(jarPath.toFile(), true)) {
            outputStream.write(parameterObject.getPartData(), 0, parameterObject.getPartSize());
        }

        if (LOGGER.isInfoEnabled()) {
            String message = String.format("Session : %s jarPath: %s PartNumber: %d/%d Total file size : %d bytes",
                    parameterObject.getSessionId(), jarPath, currentPart, totalPart, Files.size(jarPath));
            LOGGER.info(message);
        }

        JobMetaDataParameterObject result = null;
        // If parts are complete
        if (currentPart == totalPart) {

            validateJarChecksum();

            result = jobMetaDataParameterObject;
        }
        return result;
    }

    void createNewTemporaryFile() throws IOException {
        Path jarPath = createJarPath();

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

        // Keep the temporary file's path in the metadata object
        jobMetaDataParameterObject.setJarPath(jarPath);
    }

    @SuppressWarnings("java:S5443")
    Path createJarPath() throws IOException {
        Path jarPath;
        if (jobMetaDataParameterObject.getUploadDirectoryPath() != null) {
            Path path = Paths.get(jobMetaDataParameterObject.getUploadDirectoryPath());
            // Create a new temporary file in the given directory
            jarPath = Files.createTempFile(path, jobMetaDataParameterObject.getFileName(), ".jar");
        } else {
            // Create a new temporary file in the default temporary file directory
            jarPath = Files.createTempFile(jobMetaDataParameterObject.getFileName(), ".jar");
        }
        return jarPath;
    }

    private static void validateReceivedParameters(JobMultiPartParameterObject parameterObject) {
        int receivedCurrentPart = parameterObject.getCurrentPartNumber();
        int receivedTotalPart = parameterObject.getTotalPartNumber();

        // Ensure positive number
        if (receivedCurrentPart <= 0) {
            String errorMessage = String.format("receivedPart : %d is incorrect", receivedCurrentPart);
            throw new JetException(errorMessage);
        }

        // Ensure positive number
        if (receivedTotalPart <= 0) {
            String errorMessage = String.format("receivedTotalPart : %d is incorrect", receivedTotalPart);
            throw new JetException(errorMessage);
        }

        // Ensure received values are logical
        if (receivedCurrentPart > receivedTotalPart) {
            String errorMessage = String.format("receivedPart : %d is bigger than receivedTotalPart : %d ",
                    receivedCurrentPart, receivedTotalPart);
            throw new JetException(errorMessage);
        }

        // Ensure part data is not null
        byte[] partData = parameterObject.getPartData();
        if (partData == null) {
            String errorMessage = "receivedPartData is null";
            throw new JetException(errorMessage);
        }

        // Ensure part data size is positive
        if (partData.length == 0) {
            String errorMessage = "receivedPartData size is 0";
            throw new JetException(errorMessage);
        }

        // Ensure part size is positive
        int partSize = parameterObject.getPartSize();
        if (partSize == 0) {
            String errorMessage = "receivedPartSize is 0";
            throw new JetException(errorMessage);
        }

        // Ensure part size is valid
        if (partSize > partData.length) {
            String errorMessage = String.format("receivedPartSize: %d is bigger than receivedPartLength : %d ", partSize,
                    partData.length);
            throw new JetException(errorMessage);
        }

        // Ensure SHA256 of part is not null
        if (Objects.isNull(parameterObject.getSha256Hex())) {
            String errorMessage = "Sha256Hex of part is null";
            throw new JetException(errorMessage);
        }
    }

    private void validateReceivedPartNumbersAreExpected(JobMultiPartParameterObject parameterObject) {
        int receivedCurrentPart = parameterObject.getCurrentPartNumber();
        int receivedTotalPart = parameterObject.getTotalPartNumber();

        // Ensure received part is not from the past
        if (currentPart >= receivedCurrentPart) {
            String errorMessage = String.format("Received an old order part. currentPart : %d receivedPart : %d",
                    currentPart, receivedCurrentPart);
            throw new JetException(errorMessage);
        }

        // Ensure received part is the next one
        if (currentPart + 1 != receivedCurrentPart) {
            String errorMessage = String.format("Received an out of order part. currentPart : %d receivedPart : %d",
                    currentPart, receivedCurrentPart);
            throw new JetException(errorMessage);
        }

        // Ensure total part is still consistent
        if (totalPart != 0 && totalPart != receivedTotalPart) {
            String errorMessage = String.format("Received a different totalPart. totalPart : %d receivedTotalPart : %d",
                    totalPart, receivedTotalPart);
            throw new JetException(errorMessage);
        }
    }

    private void validatePartChecksum(JobMultiPartParameterObject parameterObject) throws NoSuchAlgorithmException {
        String calculatedSha256Hex = calculateSha256Hex(parameterObject.getPartData(), parameterObject.getPartSize());
        String receivedSha256Hex = parameterObject.getSha256Hex();

        if (!calculatedSha256Hex.equals(receivedSha256Hex)) {
            String errorMessage = String.format("Checksum is different!. Calculated SHA256 : %s. Received SHA256 : %s",
                    calculatedSha256Hex, receivedSha256Hex);
            throw new JetException(errorMessage);
        }
    }

    // Validate checksum when the upload is complete
    private void validateJarChecksum() throws IOException, NoSuchAlgorithmException {
        String calculatedSha256Hex = Sha256Util.calculateSha256Hex(jobMetaDataParameterObject.getJarPath());
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
