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

import com.hazelcast.jet.JetException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Holds the state of jobs that are being uploaded. Once the upload is complete the
 * state is removed. If upload is abandoned the state is removed after it expires.
 */
public class JobUploadStore {

    private static ILogger logger = Logger.getLogger(JobUploadStore.class);

    // Key is sessionID
    private ConcurrentHashMap<UUID, JobUploadStatus> jobMap = new ConcurrentHashMap<>();

    /**
     * Iterate over entries and remove expired ones
     */
    public void cleanExpiredUploads() {
        jobMap.forEach((key, value) -> {
            if (value.isExpired()) {
                removeBadSession(key);
            }
        });
    }

    /**
     * Remove the bad JobUploadStatus because of an exception or expiration
     *
     * @param sessionId specifies the key to be used
     */
    public JobUploadStatus removeBadSession(UUID sessionId) {
        JobUploadStatus jobUploadStatus = jobMap.remove(sessionId);

        if (jobUploadStatus != null) {
            jobUploadStatus.onRemove();
        }
        return jobUploadStatus;
    }

    /**
     * Process the metadata for the job upload
     *
     * @param parameterObject specifies the first message for the job upload
     */
    public void processJobMetaData(JobMetaDataParameterObject parameterObject) throws IOException {
        UUID sessionId = parameterObject.getSessionId();
        String message = String.format("processJobMetaData : Session : %s ", sessionId);
        logger.info(message);

        if (jobMap.containsKey(sessionId)) {
            throw new JetException("Session already exists. sessionID " + sessionId);
        }

        // Create a new JobUploadStatus object and save parameters
        JobUploadStatus jobUploadStatus = jobMap.computeIfAbsent(parameterObject.getSessionId(),
                key -> new JobUploadStatus(parameterObject));
        jobUploadStatus.createNewTemporaryFile();

    }

    /**
     * Process the part message for the job upload
     *
     * @param parameterObject specified message from client
     * @return JobMetaDataParameterObject if upload is complete
     * @throws IOException              in case of I/O error
     * @throws NoSuchAlgorithmException in case of message digest error
     */
    public JobMetaDataParameterObject processJobMultipart(JobMultiPartParameterObject parameterObject)
            throws IOException, NoSuchAlgorithmException {
        // Log parameterObject
        UUID sessionId = parameterObject.getSessionId();
        int currentPart = parameterObject.getCurrentPartNumber();
        int totalPart = parameterObject.getTotalPartNumber();
        String message = String.format("processJobMultipart : Session : %s Received : %d of %d", sessionId, currentPart,
                totalPart);
        logger.info(message);

        JobUploadStatus jobUploadStatus = jobMap.get(sessionId);
        if (jobUploadStatus == null) {
            throw new JetException("Unknown session id : " + sessionId);
        }

        JobMetaDataParameterObject partsComplete = jobUploadStatus.processJobMultipart(parameterObject);

        // If job upload is complete
        if (partsComplete != null) {

            message = String.format("Session : %s is complete. It will be removed from the map", sessionId);
            logger.info(message);

            // Remove from the map so that it does not expire
            jobMap.remove(partsComplete.getSessionId());
        }
        return partsComplete;
    }
}
