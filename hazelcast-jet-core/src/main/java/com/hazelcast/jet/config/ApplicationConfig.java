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

package com.hazelcast.jet.config;


import java.io.Serializable;
import java.util.Properties;

import static com.hazelcast.util.Preconditions.checkTrue;

/**
 * Config for a {@link com.hazelcast.jet.application.Application}
 */
public class ApplicationConfig implements Serializable {

    /**
     * Represents default connection checking interval
     */
    public static final int DEFAULT_CONNECTIONS_CHECKING_INTERVAL_MS = 100000;

    /**
     * Represents default value for timeout when socket accepted as broken
     */
    public static final int DEFAULT_CONNECTIONS_SILENCE_TIMEOUT_MS = 1000;

    /**
     * Represents default number of attempts to create localization directories
     */
    public static final int DEFAULT_APP_ATTEMPTS_COUNT = 100;

    /**
     * Represents default value for localization process chunks to be used
     */
    public static final int DEFAULT_FILE_CHUNK_SIZE_BYTES = 1024;

    /**
     * Default chunk size for data passed between JET-containers
     */
    public static final int DEFAULT_CHUNK_SIZE = 256;

    /**
     * Represents default value for TCP-buffer
     */
    public static final int DEFAULT_TCP_BUFFER_SIZE = 1024;

    /**
     * Default size for the queues used to pass data between containers
     */
    private static final int DEFAULT_QUEUE_SIZE = 65536;

    /**
     * Default packet-size to be used during transportation process
     */
    private static final int DEFAULT_SHUFFLING_BATCH_SIZE_BYTES = 256;

    private final Properties properties;

    private String localizationDirectory;

    private int applicationDirectoryCreationAttemptsCount = DEFAULT_APP_ATTEMPTS_COUNT;

    private int secondsToAwait = JetConfig.DEFAULT_SECONDS_TO_AWAIT;

    private int ringbufferSize = DEFAULT_QUEUE_SIZE;

    private int chunkSize = DEFAULT_CHUNK_SIZE;

    private int tcpBufferSize = DEFAULT_TCP_BUFFER_SIZE;

    private int shufflingBatchSizeBytes = DEFAULT_SHUFFLING_BATCH_SIZE_BYTES;

    private String name;

    /**
     * Constructs an empty ApplicationConfig
     */
    public ApplicationConfig() {
        this.name = null;
        this.properties = new Properties();
    }

    /**
     * Constructs an ApplicationConfig with the given name
     *
     * @param name name of the application
     */
    public ApplicationConfig(String name) {
        this();
        this.name = name;
    }

    /**
     * Returns the name of the application
     *
     * @return the name of the application
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of the application
     *
     * @param name name of the application
     * @return the current application configuration
     */
    public ApplicationConfig setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * Gets the name of the directory to be used for localization
     *
     * @return the name of the directory
     */
    public String getLocalizationDirectory() {
        return localizationDirectory;
    }

    /**
     * Sets the name of the directory to be used for localization
     *
     * @param localizationDirectory name of the directory
     * @return the current application configuration
     */
    public ApplicationConfig setLocalizationDirectory(String localizationDirectory) {
        this.localizationDirectory = localizationDirectory;
        return this;
    }

    /**
     * Gets the maximum number of attempts to create a temp directory during localization
     * @return the number of attempts
     */
    public int getApplicationDirectoryCreationAttemptsCount() {
        return applicationDirectoryCreationAttemptsCount;
    }

    /**
     * Sets the maximum number of attempts to create a temp directory during localization
     * @param count the maximum number of attempts
     * @return the current application configuration
     */
    public ApplicationConfig setApplicationDirectoryCreationAttemptsCount(
            int count) {
        this.applicationDirectoryCreationAttemptsCount = count;
        return this;
    }

    /**
     * Gets the internal timeout of the application
     *
     * @return the internal timeout of the application
     */
    public int getSecondsToAwait() {
        return secondsToAwait;
    }

    /**
     * Sets the internal timeout of the application
     *
     * @param secondsToAwait the internal timeout of the application
     * @return the current application configuration
     */
    public ApplicationConfig setSecondsToAwait(int secondsToAwait) {
        this.secondsToAwait = secondsToAwait;
        return this;
    }

    /**
     * Gets the size of the ringbuffer used when passing data between processors
     *
     * @return the size of the ringbuffer
     */
    public int getRingbufferSize() {
        return ringbufferSize;
    }

    /**
     * Sets the size of the ringbuffer used when passing data between processors
     *
     * @param ringbufferSize the size of the ringbuffer
     * @return the current application configuration
     */
    public ApplicationConfig setRingbufferSize(int ringbufferSize) {
        checkTrue(Integer.bitCount(ringbufferSize) == 1, "ringbufferSize should be power of 2");
        this.ringbufferSize = ringbufferSize;
        return this;
    }

    /**
     * Gets the size of the chunk that will be processed at each call in {@code ContainerProcessor.process}
     *
     * @return the chunk size
     */
    public int getChunkSize() {
        return chunkSize;
    }

    /**
     * Sets the size of the chunk that will be processed at each call in {@code ContainerProcessor.process}
     * @param chunkSize the chunk size
     * @return the current application configuration
     */
    public ApplicationConfig setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
        return this;
    }

    /**
     * Gets the size of the batch to send when shuffling data to other nodes
     * @return the size of the batch to send when shuffling data to other nodes
     */
    public int getShufflingBatchSizeBytes() {
        return shufflingBatchSizeBytes;
    }

    /**
     * Sets the size of the batch to send when shuffling data to other nodes
     * @param shufflingBatchSizeBytes the size of the batch to send when shuffling data to other nodes
     * @return the current application configuration
     */
    public ApplicationConfig setShufflingBatchSizeBytes(int shufflingBatchSizeBytes) {
        this.shufflingBatchSizeBytes = shufflingBatchSizeBytes;
        return this;
    }

    /**
     * Gets application specific properties
     * @return application specific properties
     */
    public Properties getProperties() {
        return properties;
    }

    /**
     * Get the TCP buffer size used when writing to network
     * @return the TCP buffer size
     */
    public int getTcpBufferSize() {
        return tcpBufferSize;
    }

    /**
     * Set the TCP buffer size used when writing to network
     * @param tcpBufferSize the TCP buffer size
     * @return the current application configuration
     */
    public ApplicationConfig setTcpBufferSize(int tcpBufferSize) {
        this.tcpBufferSize = tcpBufferSize;
        return this;
    }

}
