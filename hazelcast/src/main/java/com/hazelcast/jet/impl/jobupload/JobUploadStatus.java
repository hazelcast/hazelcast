package com.hazelcast.jet.impl.jobupload;

import com.hazelcast.jet.JetException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;

public class JobUploadStatus {
    Instant lastUpdatedTime;

    int currentPart;
    int totalPart;

    RunJarParameterObject parameterObject;

    public Instant getLastUpdatedTime() {
        return lastUpdatedTime;
    }

    public void setLastUpdatedTime(Instant lastUpdatedTime) {
        this.lastUpdatedTime = lastUpdatedTime;
    }

    public RunJarParameterObject getParameterObject() {
        return parameterObject;
    }

    public void setParameterObject(RunJarParameterObject parameterObject) {
        this.parameterObject = parameterObject;
    }

    public boolean processJarData(int receivedCurrentPart, int receivedTotalPart, byte[] jarData) throws IOException {

        if (currentPart >= receivedCurrentPart) {
            String errorMessage = String.format("Received an out of order part. currentPart : %d receivedPart : %d",
                    currentPart, receivedCurrentPart);
            throw new JetException(errorMessage);
        }

        if (totalPart != receivedTotalPart) {
            String errorMessage = String.format("Received a different totalPart. totalPart : %d receivedTotalPart : %d",
                    totalPart, receivedTotalPart);
            throw new JetException(errorMessage);
        }

        currentPart = receivedCurrentPart;
        totalPart = receivedTotalPart;

        // If the first part
        if (currentPart == 1) {
            //Create a new temporary file
            Path tempFile = Files.createTempFile("runjob", ".jar");
            parameterObject.setJarPath(tempFile);
        }

        Files.write(parameterObject.getJarPath(), jarData, StandardOpenOption.APPEND);

        //Return if parts are complete
        return currentPart == totalPart;
    }
}
