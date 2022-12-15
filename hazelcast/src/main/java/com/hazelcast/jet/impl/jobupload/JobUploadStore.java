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

    public void processJarMetaData(UUID sessionId, RunJarParameterObject parameterObject) {

        // Create a new JobUploadStatus object and save parameters
        jobMap.computeIfAbsent(sessionId, key -> new JobUploadStatus(sessionId, parameterObject));

    }

    public boolean processJarData(UUID sessionId, int currentPart, int totalPart, byte[] jarData) throws IOException {
        JobUploadStatus jobUploadStatus = jobMap.get(sessionId);
        if (jobUploadStatus == null) {
            throw new JetException("Unknown session id : " + sessionId);
        }
        String message = String.format("Session : %s Received : %d of %d", sessionId, currentPart, totalPart);
        logger.info(message);

        return jobUploadStatus.processJarData(currentPart, totalPart, jarData);
    }


}
