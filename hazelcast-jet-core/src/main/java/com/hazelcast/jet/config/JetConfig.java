/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.map.merge.IgnoreMergingEntryMapMergePolicy;

import java.util.List;
import java.util.Properties;

import static com.hazelcast.spi.partition.IPartition.MAX_BACKUP_COUNT;
import static java.util.Arrays.asList;

/**
 * Configuration object for a Jet instance.
 */
public class JetConfig {

    /**
     * Name of internal IMap which stores job ids
     */
    public static final String IDS_MAP_NAME = "__jet.jobs.ids";

    /**
     * Name of internal IMap which stores job resources
     */
    public static final String RESOURCES_MAP_NAME_PREFIX = "__jet.jobs.resources.";

    /**
     * Name of internal IMap which stores job records
     */
    public static final String JOB_RECORDS_MAP_NAME = "__jet.jobs.records";

    /**
     * Name of internal IMap which stores job results
     */
    public static final String JOB_RESULTS_MAP_NAME = "__jet.jobs.results";

    /**
     * The default port number for the cluster auto-discovery mechanism's
     * multicast communication.
     */
    public static final int DEFAULT_JET_MULTICAST_PORT = 54326;

    /**
     * The default backup count to be used for storing job metadata in Hazelcast maps
     */
    public static final int JOB_METADATA_DEFAULT_BACKUP_COUNT = MapConfig.DEFAULT_BACKUP_COUNT;


    private Config hazelcastConfig = defaultHazelcastConfig();
    private InstanceConfig instanceConfig = new InstanceConfig();
    private EdgeConfig defaultEdgeConfig = new EdgeConfig();
    private Properties properties = new Properties();
    private int jobMetadataBackupCount = JOB_METADATA_DEFAULT_BACKUP_COUNT;

    /**
     * Returns the configuration object for the underlying Hazelcast instance.
     */
    public Config getHazelcastConfig() {
        return hazelcastConfig;
    }

    /**
     * Sets the underlying IMDG instance's configuration object.
     */
    public JetConfig setHazelcastConfig(Config config) {
        hazelcastConfig = config;
        setInternalMapMergePolicies(config);
        setJobMetadataBackupCount(jobMetadataBackupCount);
        return this;
    }

    /**
     * Returns the Jet instance config.
     */
    public InstanceConfig getInstanceConfig() {
        return instanceConfig;
    }

    /**
     * Sets the Jet instance config.
     */
    public JetConfig setInstanceConfig(InstanceConfig instanceConfig) {
        this.instanceConfig = instanceConfig;
        return this;
    }

    /**
     * Returns the Jet-specific configuration properties.
     */
    public Properties getProperties() {
        return properties;
    }

    /**
     * Sets the Jet-specific configuration properties.
     */
    public JetConfig setProperties(Properties properties) {
        this.properties = properties;
        return this;
    }

    /**
     * Returns the default DAG edge configuration.
     */
    public EdgeConfig getDefaultEdgeConfig() {
        return defaultEdgeConfig;
    }

    /**
     * Sets the configuration object that specifies the defaults to use
     * for a DAG edge configuration.
     */
    public JetConfig setDefaultEdgeConfig(EdgeConfig defaultEdgeConfig) {
        this.defaultEdgeConfig = defaultEdgeConfig;
        return this;
    }

    /**
     * Sets the backup count which is used for storing job metadata objects
     */
    public JetConfig setJobMetadataBackupCount(int newBackupCount) {
        if (newBackupCount < 0) {
            throw new IllegalArgumentException("backup-count can't be smaller than 0");
        } else if (newBackupCount > MAX_BACKUP_COUNT) {
            throw new IllegalArgumentException("backup-count can't be larger than than " + MAX_BACKUP_COUNT);
        }
        internalJetMapNames().forEach(name -> hazelcastConfig.getMapConfig(name).setBackupCount(newBackupCount));
        this.jobMetadataBackupCount = newBackupCount;
        return this;
    }

    /**
     * Returns the backup count which is used for storing job metadata objects
     */
    public int getJobMetadataBackupCount() {
        return jobMetadataBackupCount;
    }

    private static Config defaultHazelcastConfig() {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setMulticastPort(DEFAULT_JET_MULTICAST_PORT);
        config.getGroupConfig().setName("jet");
        config.getGroupConfig().setPassword("jet-pass");
        setInternalMapMergePolicies(config);
        return config;
    }

    private static void setInternalMapMergePolicies(Config config) {
        String mergePolicy = IgnoreMergingEntryMapMergePolicy.class.getName();
        internalJetMapNames().forEach(name -> config.getMapConfig(name).setMergePolicy(mergePolicy));
    }

    private static List<String> internalJetMapNames() {
        String resourcesMapNameWildcard = RESOURCES_MAP_NAME_PREFIX + "*";
        return asList(IDS_MAP_NAME, JOB_RECORDS_MAP_NAME, resourcesMapNameWildcard, JOB_RESULTS_MAP_NAME);
    }

}
