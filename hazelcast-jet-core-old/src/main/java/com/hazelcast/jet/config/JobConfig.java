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


import com.hazelcast.jet.Job;
import com.hazelcast.jet.impl.job.deployment.DeploymentType;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static com.hazelcast.jet.impl.util.JetUtil.unchecked;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkTrue;

/**
 * Config for a {@link Job}
 */
@SuppressWarnings("checkstyle:methodcount")
public class JobConfig implements Serializable {

    /**
     * Represents default connection checking interval
     */
    public static final int DEFAULT_CONNECTIONS_CHECKING_INTERVAL_MS = 100000;

    /**
     * Represents default value for timeout when socket accepted as broken
     */
    public static final int DEFAULT_CONNECTIONS_SILENCE_TIMEOUT_MS = 1000;

    /**
     * Represents default number of attempts to create deployment directories
     */
    public static final int DEFAULT_APP_ATTEMPTS_COUNT = 100;

    /**
     * Default chunk size for data passed between JET-runners
     */
    public static final int DEFAULT_CHUNK_SIZE = 256;

    /**
     * Represents default value for TCP-buffer
     */
    public static final int DEFAULT_TCP_BUFFER_SIZE = 1024;

    /**
     * Default size for the queues used to pass data between runners
     */
    private static final int DEFAULT_QUEUE_SIZE = 65536;

    /**
     * Default packet-size to be used during transportation process
     */
    private static final int DEFAULT_SHUFFLING_BATCH_SIZE_BYTES = 256;

    private final Properties properties;

    private String deploymentDirectory;

    private int secondsToAwait = JetConfig.DEFAULT_SECONDS_TO_AWAIT;

    private int ringbufferSize = DEFAULT_QUEUE_SIZE;

    private int chunkSize = DEFAULT_CHUNK_SIZE;

    private int tcpBufferSize = DEFAULT_TCP_BUFFER_SIZE;

    private int shufflingBatchSizeBytes = DEFAULT_SHUFFLING_BATCH_SIZE_BYTES;

    private String name;

    private Set<DeploymentConfig> deploymentConfigs = new HashSet<>();

    /**
     * Constructs an empty JobConfig
     */
    public JobConfig() {
        this.name = null;
        this.properties = new Properties();
    }

    /**
     * Constructs an JobConfig with the given name
     *
     * @param name name of the job
     */
    public JobConfig(String name) {
        this();
        this.name = name;
    }

    /**
     * Returns the name of the job
     *
     * @return the name of the job
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of the job
     *
     * @param name name of the job
     * @return the current job configuration
     */
    public JobConfig setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * Gets the name of the directory to be used for deployment
     *
     * @return the name of the directory
     */
    public String getDeploymentDirectory() {
        return deploymentDirectory;
    }

    /**
     * Sets the name of the directory to be used for deployment
     *
     * @param deploymentDirectory name of the directory
     * @return the current job configuration
     */
    public JobConfig setDeploymentDirectory(String deploymentDirectory) {
        this.deploymentDirectory = deploymentDirectory;
        return this;
    }

    /**
     * Gets the internal timeout of the job
     *
     * @return the internal timeout of the job
     */
    public int getSecondsToAwait() {
        return secondsToAwait;
    }

    /**
     * Sets the internal timeout of the job
     *
     * @param secondsToAwait the internal timeout of the job
     * @return the current job configuration
     */
    public JobConfig setSecondsToAwait(int secondsToAwait) {
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
     * @return the current job configuration
     */
    public JobConfig setRingbufferSize(int ringbufferSize) {
        checkTrue(Integer.bitCount(ringbufferSize) == 1, "ringbufferSize should be power of 2");
        this.ringbufferSize = ringbufferSize;
        return this;
    }

    /**
     * Gets the size of the chunk that will be processed at each call in {@code Processor.process}
     *
     * @return the chunk size
     */
    public int getChunkSize() {
        return chunkSize;
    }

    /**
     * Sets the size of the chunk that will be processed at each call in {@code Processor.process}
     *
     * @param chunkSize the chunk size
     * @return the current job configuration
     */
    public JobConfig setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
        return this;
    }

    /**
     * Gets the size of the batch to send when shuffling data to other nodes
     *
     * @return the size of the batch to send when shuffling data to other nodes
     */
    public int getShufflingBatchSizeBytes() {
        return shufflingBatchSizeBytes;
    }

    /**
     * Sets the size of the batch to send when shuffling data to other nodes
     *
     * @param shufflingBatchSizeBytes the size of the batch to send when shuffling data to other nodes
     * @return the current job configuration
     */
    public JobConfig setShufflingBatchSizeBytes(int shufflingBatchSizeBytes) {
        this.shufflingBatchSizeBytes = shufflingBatchSizeBytes;
        return this;
    }

    /**
     * Gets job specific properties
     *
     * @return job specific properties
     */
    public Properties getProperties() {
        return properties;
    }

    /**
     * Get the TCP buffer size used when writing to network
     *
     * @return the TCP buffer size
     */
    public int getTcpBufferSize() {
        return tcpBufferSize;
    }

    /**
     * Set the TCP buffer size used when writing to network
     *
     * @param tcpBufferSize the TCP buffer size
     * @return the current job configuration
     */
    public JobConfig setTcpBufferSize(int tcpBufferSize) {
        this.tcpBufferSize = tcpBufferSize;
        return this;
    }

    /**
     * Add class to the job classLoader
     *
     * @param classes classes, which will be used during calculation
     */
    public void addClass(Class... classes) {
        checkNotNull(classes, "Classes can not be null");

        for (Class clazz : classes) {
            try {
                deploymentConfigs.add(new DeploymentConfig(clazz));
            } catch (IOException e) {
                throw unchecked(e);
            }
        }
    }

    /**
     * Add JAR to the job classLoader
     *
     * @param url location of the JAR file
     */
    public void addJar(URL url) {
        addJar(url, getFileName(url));
    }

    /**
     * Add JAR to the job classLoader
     *
     * @param url location of the JAR file
     * @param id  identifier for the JAR file
     */
    public void addJar(URL url, String id) {
        add(url, id, DeploymentType.JAR);
    }

    /**
     * Add JAR to the job classLoader
     *
     * @param file the JAR file
     */
    public void addJar(File file) {
        try {
            addJar(file.toURI().toURL(), file.getName());
        } catch (MalformedURLException e) {
            throw unchecked(e);
        }
    }

    /**
     * Add JAR to the job classLoader
     *
     * @param file the JAR file
     * @param id   identifier for the JAR file
     */
    public void addJar(File file, String id) {
        try {
            addJar(file.toURI().toURL(), id);
        } catch (MalformedURLException e) {
            throw unchecked(e);
        }
    }

    /**
     * Add JAR to the job classLoader
     *
     * @param path path the JAR file
     */
    public void addJar(String path) {
        try {
            File file = new File(path);
            addJar(file.toURI().toURL(), file.getName());
        } catch (MalformedURLException e) {
            throw unchecked(e);
        }
    }

    /**
     * Add JAR to the job classLoader
     *
     * @param path path the JAR file
     * @param id   identifier for the JAR file
     */
    public void addJar(String path, String id) {
        try {
            addJar(new File(path).toURI().toURL(), id);
        } catch (MalformedURLException e) {
            throw unchecked(e);
        }
    }


    /**
     * Add resource to the job classLoader
     *
     * @param url source url with classes
     */
    public void addResource(URL url) {
        addResource(url, getFileName(url));

    }

    /**
     * Add resource to the job classLoader
     *
     * @param url source url with classes
     * @param id  identifier for the resource
     */
    public void addResource(URL url, String id) {
        add(url, id, DeploymentType.DATA);
    }

    /**
     * Add resource to the job classLoader
     *
     * @param file resource file
     */
    public void addResource(File file) {
        try {
            addResource(file.toURI().toURL(), file.getName());
        } catch (MalformedURLException e) {
            throw unchecked(e);
        }

    }

    /**
     * Add resource to the job classLoader
     *
     * @param file resource file
     * @param id   identifier for the resource
     */
    public void addResource(File file, String id) {
        try {
            add(file.toURI().toURL(), id, DeploymentType.DATA);
        } catch (MalformedURLException e) {
            throw unchecked(e);
        }
    }

    /**
     * Add resource to the job classLoader
     *
     * @param path path of the resource
     */
    public void addResource(String path) {
        File file = new File(path);
        try {
            addResource(file.toURI().toURL(), file.getName());
        } catch (MalformedURLException e) {
            throw unchecked(e);
        }
    }

    /**
     * Add resource to the job classLoader
     *
     * @param path path of the resource
     * @param id   identifier for the resource
     */
    public void addResource(String path, String id) {
        File file = new File(path);
        try {
            addResource(file.toURI().toURL(), id);
        } catch (MalformedURLException e) {
            throw unchecked(e);
        }
    }

    /**
     * Returns all the deployment configurations
     *
     * @return deployment configuration set
     */
    public Set<DeploymentConfig> getDeploymentConfigs() {
        return deploymentConfigs;
    }

    private void add(URL url, String id, DeploymentType type) {
        try {
            deploymentConfigs.add(new DeploymentConfig(url, id, type));
        } catch (IOException e) {
            throw unchecked(e);
        }
    }

    private String getFileName(URL url) {
        String urlFile = url.getFile();
        return urlFile.substring(urlFile.lastIndexOf('/') + 1, urlFile.length());
    }


}
