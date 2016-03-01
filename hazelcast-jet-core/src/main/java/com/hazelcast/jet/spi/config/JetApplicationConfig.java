/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.spi.config;


import java.io.Serializable;
import java.util.Properties;

import static com.hazelcast.util.Preconditions.checkTrue;

/**
 * Config for JET application;
 */
public class JetApplicationConfig implements Serializable {
    public static final int DEFAULT_PORT = 6701;

    public static final int PORT_AUTO_INCREMENT = 1;

    public static final int DEFAULT_CONNECTIONS_CHECKING_INTERVAL_MS = 100000;

    public static final int DEFAULT_CONNECTIONS_SILENCE_TIMEOUT_MS = 1000;

    public static final int DEFAULT_APP_ATTEMPTS_COUNT = 100;

    public static final int DEFAULT_FILE_CHUNK_SIZE_BYTES = 1024;

    public static final int DEFAULT_CHUNK_SIZE = 256;

    public static final int DEFAULT_JET_SECONDS_TO_AWAIT = 1200;

    public static final int DEFAULT_TCP_BUFFER_SIZE = 1024;

    private static final int DEFAULT_QUEUE_SIZE = 65536;

    private static final int DEFAULT_SHUFFLING_BATCH_SIZE_BYTES = 256;

    private static final int DEFAULT_IO_THREADS_COUNT = 5;
    private final JetApplicationConfig defConfig;
    private final Properties properties;
    private String localizationDirectory;
    private int resourceFileChunkSize = DEFAULT_FILE_CHUNK_SIZE_BYTES;
    private int defaultApplicationDirectoryCreationAttemptsCount = DEFAULT_APP_ATTEMPTS_COUNT;
    private int jetSecondsToAwait = DEFAULT_JET_SECONDS_TO_AWAIT;
    private int containerQueueSize = DEFAULT_QUEUE_SIZE;
    private int chunkSize = DEFAULT_CHUNK_SIZE;
    private int maxProcessingThreads = -1;
    private int ioThreadCount = DEFAULT_IO_THREADS_COUNT;
    private int defaultTCPBufferSize = DEFAULT_TCP_BUFFER_SIZE;
    private int shufflingBatchSizeBytes = DEFAULT_SHUFFLING_BATCH_SIZE_BYTES;
    private String name;

    public JetApplicationConfig(JetApplicationConfig defConfig, String name) {
        this.name = name;
        this.defConfig = defConfig;
        this.properties = new Properties();

    }

    public JetApplicationConfig() {
        this.defConfig = null;
        this.name = null;
        this.properties = new Properties();
    }

    public JetApplicationConfig(String name) {
        this.defConfig = null;
        this.name = name;
        this.properties = new Properties();
    }

    public JetApplicationConfig getAsReadOnly() {
        return new JetApplicationConfigReadOnly(this, name);
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getResourceFileChunkSize() {
        return resourceFileChunkSize;
    }

    public void setResourceFileChunkSize(byte resourceFileChunkSize) {
        this.resourceFileChunkSize = resourceFileChunkSize;
    }

    public String getLocalizationDirectory() {
        return localizationDirectory;
    }

    public void setLocalizationDirectory(String localizationDirectory) {
        this.localizationDirectory = localizationDirectory;
    }

    public int getDefaultApplicationDirectoryCreationAttemptsCount() {
        return defaultApplicationDirectoryCreationAttemptsCount;
    }

    public void setDefaultApplicationDirectoryCreationAttemptsCount(int defaultApplicationDirectoryCreationAttemptsCount) {
        this.defaultApplicationDirectoryCreationAttemptsCount = defaultApplicationDirectoryCreationAttemptsCount;
    }

    public int getJetSecondsToAwait() {
        return jetSecondsToAwait;
    }

    public void setJetSecondsToAwait(int jetSecondsToAwait) {
        this.jetSecondsToAwait = jetSecondsToAwait;
    }

    public int getContainerQueueSize() {
        return containerQueueSize;
    }

    public void setContainerQueueSize(int containerQueueSize) {
        checkTrue(Integer.bitCount(containerQueueSize) == 1, "containerQueueSize should be power of 2");
        this.containerQueueSize = containerQueueSize;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(int tupleChunkSize) {
        this.chunkSize = tupleChunkSize;
    }

    public int getMaxProcessingThreads() {
        return maxProcessingThreads <= 0 ? Runtime.getRuntime().availableProcessors() : maxProcessingThreads;
    }

    public void setMaxProcessingThreads(int maxProcessingThreads) {
        this.maxProcessingThreads = maxProcessingThreads;
    }

    public int getShufflingBatchSizeBytes() {
        return shufflingBatchSizeBytes;
    }

    public void setShufflingBatchSizeBytes(int shufflingBatchSizeBytes) {
        this.shufflingBatchSizeBytes = shufflingBatchSizeBytes;
    }

    public Properties getProperties() {
        return properties;
    }

    public int getIoThreadCount() {
        return ioThreadCount;
    }

    public void setIoThreadCount(int ioThreadCount) {
        this.ioThreadCount = ioThreadCount;
    }

    public int getDefaultTCPBufferSize() {
        return defaultTCPBufferSize;
    }

    public void setDefaultTCPBufferSize(int defaultTCPBufferSize) {
        this.defaultTCPBufferSize = defaultTCPBufferSize;
    }
}
