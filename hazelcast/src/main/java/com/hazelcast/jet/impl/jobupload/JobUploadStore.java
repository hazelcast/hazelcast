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

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class JobUploadStore {

    private static ILogger logger = Logger.getLogger(JobUploadStore.class);

    private ConcurrentHashMap<UUID, JobUploadStatus> jobMap = new ConcurrentHashMap<>();

    // Iterate over entries and remove expired ones
    public void cleanExpiredUploads() {
        jobMap.forEach((key, value) -> {
            if (value.isExpired()) {
                remove(key);
            }
        });
    }

    public void remove(UUID sessionId) {
        JobUploadStatus jobUploadStatus = jobMap.remove(sessionId);

        if (jobUploadStatus != null) {
            jobUploadStatus.onRemove();
        }
    }

    public void processJarMetaData(JobMetaDataParameterObject parameterObject) {
        UUID sessionId = parameterObject.getSessionId();
        String message = String.format("processJarMetaData : Session : %s ",sessionId);
        logger.info(message);

        // Create a new JobUploadStatus object and save parameters
        jobMap.computeIfAbsent(parameterObject.getSessionId(), key -> new JobUploadStatus(parameterObject));

    }

    public JobMetaDataParameterObject processJobMultipart(JobMultiPartParameterObject parameterObject)
            throws IOException {
        // Log parameterObject
        UUID sessionId = parameterObject.getSessionId();
        int currentPart = parameterObject.getCurrentPartNumber();
        int totalPart = parameterObject.getTotalPartNumber();
        String message = String.format("processJobMultipart : Session : %s Received : %d of %d", sessionId, currentPart, totalPart);
        logger.info(message);

        JobUploadStatus jobUploadStatus = jobMap.get(sessionId);
        if (jobUploadStatus == null) {
            throw new JetException("Unknown session id : " + sessionId);
        }


        JobMetaDataParameterObject partsComplete = jobUploadStatus.processJarData(parameterObject);

        // If job upload is complete, remote from the map
        if(partsComplete != null) {

            message = String.format("Session : %s is complete. It will be removed from the map", sessionId);
            logger.info(message);

            jobMap.remove(partsComplete.getSessionId());
        }
        return partsComplete;
    }
}
