package com.hazelcast.jet.impl.jobupload;

import com.hazelcast.jet.JetException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class JobUploadStatus {

    private static final ILogger logger = Logger.getLogger(JobUploadStatus.class);

    private final UUID sessionId;
    protected long lastUpdatedTime = System.nanoTime();

    protected static final long EXPIRATION_MINUTES = 1;
    private int currentPart;
    private int totalPart;

    private final RunJarParameterObject parameterObject;

    public JobUploadStatus(UUID sessionId, RunJarParameterObject parameterObject) {
        this.sessionId = sessionId;
        this.parameterObject = parameterObject;
    }

    public boolean isExpired() {
        long elapsedTime = NANOSECONDS.toMinutes(System.nanoTime() - lastUpdatedTime);

        return elapsedTime >= EXPIRATION_MINUTES;
    }

    public void onRemove() {
        try {
            if (parameterObject.getJarPath() != null) {
                Files.delete(parameterObject.getJarPath());
            }
        } catch (IOException exception) {
            logger.severe("Error while deleting file : " + parameterObject.getJarPath(), exception);
        }
    }

    public boolean processJarData(int receivedCurrentPart, int receivedTotalPart, byte[] jarData) throws IOException {

        ensureReceivedPartNumbersAreValid(receivedCurrentPart, receivedTotalPart);

        ensureReceivedPartNumbersAreExpected(receivedCurrentPart, receivedTotalPart);

        // Parts numbers are good. Save them
        currentPart = receivedCurrentPart;
        totalPart = receivedTotalPart;

        Path jarPath = parameterObject.getJarPath();

        // If the first part
        if (currentPart == 1) {
            //Create a new temporary file
            jarPath = Files.createTempFile("runjob", ".jar");
            parameterObject.setJarPath(jarPath);
        }

        // Append data to file
        Files.write(jarPath, jarData, StandardOpenOption.APPEND);

        String message = String.format("Session : %s total file size %d", sessionId, Files.size(jarPath));
        logger.info(message);

        changeLastUpdatedTime();

        //Return if parts are complete
        return currentPart == totalPart;
    }

    private static void ensureReceivedPartNumbersAreValid(int receivedCurrentPart, int receivedTotalPart) {
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

    private void ensureReceivedPartNumbersAreExpected(int receivedCurrentPart, int receivedTotalPart) {
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
        this.lastUpdatedTime = System.nanoTime();
    }
}
