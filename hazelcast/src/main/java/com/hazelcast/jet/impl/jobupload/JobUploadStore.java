package com.hazelcast.jet.impl.jobupload;

import com.hazelcast.jet.JetException;
import com.hazelcast.logging.ILogger;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class JobUploadStore {

    private ILogger logger;

    protected ConcurrentHashMap<UUID, JobUploadStatus> jobStore = new ConcurrentHashMap<>();

    public void setLogger(ILogger logger) {
        this.logger = logger;
    }

    public void cancel(UUID sessionId) {
        JobUploadStatus jobUploadStatus = jobStore.remove(sessionId);

        if (jobUploadStatus != null) {
            RunJarParameterObject parameterObject = jobUploadStatus.getParameterObject();
            try {
                Files.delete(parameterObject.getJarPath());
            } catch (IOException exception) {
                logger.severe("Enable to delete file : " + parameterObject.getJarPath());
            }
        }
    }

    public void processJarMetaData(UUID sessionId, RunJarParameterObject parameterObject) {

        jobStore.computeIfAbsent(sessionId, (key) -> {
            JobUploadStatus jobUploadStatus = new JobUploadStatus();
            jobUploadStatus.setLastUpdatedTime(Instant.now());
            jobUploadStatus.setParameterObject(parameterObject);
            return jobUploadStatus;
        });
    }

    public boolean processJarData(UUID sessionId, int currentPart, int totalPart, byte[] jarData) throws IOException {
        JobUploadStatus jobUploadStatus = jobStore.get(sessionId);
        if (jobUploadStatus == null) {
            throw new JetException("Unknown session id " + sessionId);
        }
        return jobUploadStatus.processJarData(currentPart, totalPart, jarData);
    }


}
