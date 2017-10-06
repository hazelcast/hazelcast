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

import java.io.File;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Job-specific configuration options.
 */
public class JobConfig implements Serializable {

    private ProcessingGuarantee processingGuarantee = ProcessingGuarantee.EXACTLY_ONCE;
    private long snapshotIntervalMillis = -1;

    private boolean splitBrainProtectionEnabled;
    private final List<ResourceConfig> resourceConfigs = new ArrayList<>();
    private boolean autoRestartEnabled = true;

    /**
     * Returns true if {@link #setSplitBrainProtection(boolean) split brain protection}
     * is enabled.
     */
    public boolean isSplitBrainProtectionEnabled() {
        return splitBrainProtectionEnabled;
    }

    /**
     * Configures the split brain protection feature. When enabled, the job will
     * only be restarted after a topology change if the quorum size can be met.
     * The quorum size is calculated as
     * {@code cluster size at job submission time / 2 + 1}. As a result,
     * the job can only be restarted if the actual cluster size is at least
     * the quorum value.
     * <p>
     * For example, if at the time of job submission the cluster size is 5 and
     * a network partition splits the cluster into two sub-clusters with one
     * having size 3 and the other 2, the job will only be restarted on the
     * sub-cluster with size 3.
     * <p>
     * Note that if you add new nodes to the cluster after a job has already
     * started the quorum size might be invalidated by the new cluster size.
     * For instance, if there are 5 instances at submission time
     * (i.e. the quorum value is 3) and later a new node joins, a split
     * of two equal sub-clusters of size 3 causes the job to be restarted
     * on both sub-clusters.
     * <p>
     * The default is set to {@code false}.
     * <p>
     * This setting has no effect if
     * {@link #setAutoRestartOnMemberFailure(boolean) auto restart on member failure}
     * is disabled.
     */
    public JobConfig setSplitBrainProtection(boolean isEnabled) {
        this.splitBrainProtectionEnabled = isEnabled;
        return this;
    }

    /**
     * Returns if {@link #setAutoRestartOnMemberFailure(boolean) auto restart on member failure}
     * is enabled.
     */
    public boolean isAutoRestartOnMemberFailureEnabled() {
        return this.autoRestartEnabled;
    }

    /**
     * Configure if the job should automatically restarted when one of the
     * participating nodes leave the cluster. When set to true, upon a
     * member failure the job will be automatically restarted on the
     * remaining members.
     * <p>
     * If snapshotting is enabled, the job will be restored from the latest
     * snapshot.
     * <p>
     * The default is set to {@code true}.
     */
    public JobConfig setAutoRestartOnMemberFailure(boolean isEnabled) {
        this.autoRestartEnabled = isEnabled;
        return this;
    }

    /**
     * Return current {@link #setProcessingGuarantee(ProcessingGuarantee)
     * processing guarantee}.
     */
    public ProcessingGuarantee getProcessingGuarantee() {
        return processingGuarantee;
    }

    /**
     * Set the processing guarantee, see {@link ProcessingGuarantee}.
     * If this method is not called, {@link ProcessingGuarantee#EXACTLY_ONCE}
     * is used.
     */
    public JobConfig setProcessingGuarantee(ProcessingGuarantee processingGuarantee) {
        this.processingGuarantee = processingGuarantee;
        return this;
    }

    /**
     * Return current {@link #setSnapshotIntervalMillis(long) snapshot interval}.
     */
    public long getSnapshotIntervalMillis() {
        return snapshotIntervalMillis;
    }

    /**
     * Set the snapshot interval in milliseconds. Negative value or zero means
     * snapshots are disabled. This is the interval between completion of
     * previous snapshot and the start of the new one. Snapshots are disabled
     * by default.
     * <p>
     * By default, snapshots are turned off.
     */
    public JobConfig setSnapshotIntervalMillis(long snapshotInterval) {
        this.snapshotIntervalMillis = snapshotInterval;
        return this;
    }

    /**
     * Adds the supplied classes to the list of resources that will be
     * available on the job's classpath while it's executing in the Jet
     * cluster.
     */
    public JobConfig addClass(Class... classes) {
        checkNotNull(classes, "Classes can not be null");

        for (Class clazz : classes) {
            resourceConfigs.add(new ResourceConfig(clazz));
        }
        return this;
    }

    /**
     * Adds the JAR identified by the supplied URL to the list of JARs that
     * will be a part of the job's classpath while it's executing in the Jet
     * cluster.
     */
    public JobConfig addJar(URL url) {
        return add(url, null, true);
    }

    /**
     * Adds the supplied JAR file to the list of JARs that will be a part of
     * the job's classpath while it's executing in the Jet cluster. The JAR
     * filename will be used as the ID of the resource.
     */
    public JobConfig addJar(File file) {
        try {
            return addJar(file.toURI().toURL());
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }
    }

    /**
     * Adds the JAR identified by the supplied pathname to the list of JARs
     * that will be a part of the job's classpath while it's executing in the
     * Jet cluster. The JAR filename will be used as the ID of the resource.
     */
    public JobConfig addJar(String path) {
        try {
            File file = new File(path);
            return addJar(file.toURI().toURL());
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }
    }

    /**
     * Adds the resource identified by the supplied URL to the list of
     * resources that will be on the job's classpath while it's executing in
     * the Jet cluster. The resource's filename will be used as its ID.
     */
    public JobConfig addResource(URL url) {
        return addResource(url, toFilename(url));
    }

    /**
     * Adds the resource identified by the supplied URL to the list of
     * resources that will be on the job's classpath while it's executing in
     * the Jet cluster. The resource will be registered under the supplied ID.
     */
    public JobConfig addResource(URL url, String id) {
        return add(url, id, false);
    }

    /**
     * Adds the supplied file to the list of resources that will be on the
     * job's classpath while it's executing in the Jet cluster. The resource's
     * filename will be used as its ID.
     */
    public JobConfig addResource(File file) {
        try {
            return addResource(file.toURI().toURL(), file.getName());
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }
    }

    /**
     * Adds the supplied file to the list of resources that will be on the
     * job's classpath while it's executing in the Jet cluster. The resource
     * will be registered under the supplied ID.
     */
    public JobConfig addResource(File file, String id) {
        try {
            return add(file.toURI().toURL(), id, false);
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }
    }

    /**
     * Adds the resource identified by the supplied pathname to the list of
     * resources that will be on the job's classpath while it's executing in
     * the Jet cluster. The resource's filename will be used as its ID.
     */
    public JobConfig addResource(String path) {
        return addResource(new File(path));
    }

    /**
     * Adds the resource identified by the supplied pathname to the list of
     * resources that will be on the job's classpath while it's executing in
     * the Jet cluster. The resource will be registered under the supplied ID.
     */
    public JobConfig addResource(String path, String id) {
        return addResource(new File(path), id);
    }

    /**
     * Returns all the registered resource configurations.
     */
    public List<ResourceConfig> getResourceConfigs() {
        return resourceConfigs;
    }

    private JobConfig add(URL url, String id, boolean isJar) {
        resourceConfigs.add(new ResourceConfig(url, id, isJar));
        return this;
    }

    private static String toFilename(URL url) {
        String urlFile = url.getPath();
        return urlFile.substring(urlFile.lastIndexOf('/') + 1, urlFile.length());
    }

}
