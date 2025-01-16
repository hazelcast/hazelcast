/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.config.vector;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.NamedConfig;
import com.hazelcast.config.SplitBrainPolicyAwareConfig;
import com.hazelcast.config.UserCodeNamespaceAwareConfig;
import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.internal.partition.IPartition;
import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.annotation.Beta;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.hazelcast.internal.cluster.Versions.V6_0;
import static com.hazelcast.internal.util.Preconditions.checkAsyncBackupCount;
import static com.hazelcast.internal.util.Preconditions.checkBackupCount;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static java.util.Objects.requireNonNull;

/**
 * Configuration object for a vector collection.
 *
 * @since 5.5
 */
@Beta
public class VectorCollectionConfig implements NamedConfig, IdentifiedDataSerializable, Versioned,
        SplitBrainPolicyAwareConfig ,
        UserCodeNamespaceAwareConfig<VectorCollectionConfig> {

    /**
     * The minimum number of backups
     */
    public static final int MIN_BACKUP_COUNT = 0;
    /**
     * The default number of backups
     */
    public static final int DEFAULT_BACKUP_COUNT = 1;
    /**
     * The maximum number of backups
     */
    public static final int MAX_BACKUP_COUNT = IPartition.MAX_BACKUP_COUNT;

    private String name;
    private int backupCount = DEFAULT_BACKUP_COUNT;
    private int asyncBackupCount = MIN_BACKUP_COUNT;
    private final List<VectorIndexConfig> vectorIndexConfigs = new ArrayList<>();
    private String splitBrainProtectionName;
    private MergePolicyConfig mergePolicyConfig = new MergePolicyConfig();
    private @Nullable String userCodeNamespace = DEFAULT_NAMESPACE;
    /**
     * Creates a new, empty {@code VectorCollectionConfig}.
     */
    public VectorCollectionConfig() {
    }

    /**
     * Constructs a VectorCollectionConfig with the given name.
     *
     * @param name the name of the vector collection
     */
    public VectorCollectionConfig(String name) {
        validateName(name);
        this.name = name;
    }

    /**
     * Constructs a new {@code VectorCollectionConfig} instance by copying the values from the provided configuration.
     *
     * @param config The {@link VectorCollectionConfig} instance to copy.
     *               It serves as the source of values for the new configuration.
     */
    public VectorCollectionConfig(VectorCollectionConfig config) {
        requireNonNull(config, "config must not be null.");
        this.name = config.getName();
        this.backupCount = config.getBackupCount();
        this.asyncBackupCount = config.getAsyncBackupCount();
        this.splitBrainProtectionName = config.getSplitBrainProtectionName();
        this.mergePolicyConfig = config.getMergePolicyConfig();
        this.userCodeNamespace = config.getUserCodeNamespace();
        setVectorIndexConfigs(config.getVectorIndexConfigs());
    }

    /**
     * Sets the name of the VectorCollection.
     *
     * @param name the name to set for this VectorCollection.
     */
    @Override
    public VectorCollectionConfig setName(String name) {
        validateName(name);
        this.name = name;
        return this;
    }

    /**
     * Returns the name of this VectorCollection
     *
     * @return the name of the VectorCollection
     */
    @Override
    public String getName() {
        return name;
    }

    /**
     * Adds a vector index configuration to this vector collection configuration.
     *
     * @param vectorIndexConfig the vector index configuration to add
     * @return this VectorCollectionConfig instance
     */
    public VectorCollectionConfig addVectorIndexConfig(VectorIndexConfig vectorIndexConfig) {
        requireNonNull(vectorIndexConfig, "vector index config must not be null.");
        validateIndexConfig(new ArrayList<>(vectorIndexConfigs) {{
            add(vectorIndexConfig);
        }});
        vectorIndexConfigs.add(vectorIndexConfig);
        return this;
    }

    /**
     * Retrieves the list of vector index configurations associated with this vector collection configuration.
     *
     * @return the list of vector index configurations
     */
    public List<VectorIndexConfig> getVectorIndexConfigs() {
        return vectorIndexConfigs;
    }

    /**
     * Sets the list of {@link VectorIndexConfig} instances for this vector collection configuration.
     * Clears the existing vector index configurations and replaces them with the provided list.
     *
     * @param vectorIndexConfigs The list of {@link VectorIndexConfig} instances to set.
     */
    public void setVectorIndexConfigs(List<VectorIndexConfig> vectorIndexConfigs) {
        validateIndexConfig(vectorIndexConfigs);
        this.vectorIndexConfigs.clear();
        this.vectorIndexConfigs.addAll(vectorIndexConfigs);
    }

    /**
     * Returns the backupCount for this {@code VectorCollection}
     *
     * @return the backupCount for this {@code VectorCollection}
     * @see #getAsyncBackupCount()
     * @since 6.0
     */
    public int getBackupCount() {
        return backupCount;
    }

    /**
     * Number of synchronous backups. For example, if 1 is set as the backup count,
     * then all entries of the vector collection will be copied to another JVM for fail-safety.
     * 0 means no sync backup.
     *
     * @param backupCount the number of synchronous backups to set for this {@code VectorCollection}
     * @return the updated {@link VectorCollectionConfig}
     * @see #setAsyncBackupCount(int)
     * @since 6.0
     */
    public VectorCollectionConfig setBackupCount(final int backupCount) {
        this.backupCount = checkBackupCount(backupCount, asyncBackupCount);
        return this;
    }

    /**
     * Returns the asynchronous backup count for this {@code VectorCollection}.
     *
     * @return the asynchronous backup count
     * @see #setBackupCount(int)
     * @since 6.0
     */
    public int getAsyncBackupCount() {
        return asyncBackupCount;
    }

    /**
     * Sets the number of asynchronous backups. 0 means no backups.
     *
     * @param asyncBackupCount the number of asynchronous synchronous backups to set
     * @return the updated {@link VectorCollectionConfig}
     * @throws IllegalArgumentException if asyncBackupCount smaller than
     *                                  0, or larger than the maximum number of backup or the sum of the
     *                                  backups and async backups is larger than the maximum number of backups
     * @see #setBackupCount(int)
     * @see #getAsyncBackupCount()
     * @since 6.0
     */
    public VectorCollectionConfig setAsyncBackupCount(int asyncBackupCount) {
        this.asyncBackupCount = checkAsyncBackupCount(backupCount, asyncBackupCount);
        return this;
    }

    /**
     * Returns the total number of backups: backupCount plus asyncBackupCount.
     *
     * @return the total number of backups: synchronous + asynchronous
     * @since 6.0
     */
    public int getTotalBackupCount() {
        return backupCount + asyncBackupCount;
    }

    /**
     * {@inheritDoc}
     *
     * @since 6.0
     */
    @Override
    public String getSplitBrainProtectionName() {
        return splitBrainProtectionName;
    }

    /**
     * {@inheritDoc}
     *
     * @since 6.0
     */
    @Override
    public VectorCollectionConfig setSplitBrainProtectionName(@Nullable String splitBrainProtectionName) {
        this.splitBrainProtectionName = splitBrainProtectionName;
        return this;
    }

    /**
     * {@inheritDoc}
     *
     * @since 6.0
     */
    @Override
    public MergePolicyConfig getMergePolicyConfig() {
        return mergePolicyConfig;
    }

    /**
     * {@inheritDoc}
     *
     * @since 6.0
     */
    @Override
    public VectorCollectionConfig setMergePolicyConfig(MergePolicyConfig mergePolicyConfig) {
        this.mergePolicyConfig = checkNotNull(mergePolicyConfig, "mergePolicyConfig cannot be null");
        return this;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        // RU_COMPAT_5_5
        if (out.getVersion().isGreaterOrEqual(V6_0)) {
            out.writeInt(backupCount);
            out.writeInt(asyncBackupCount);
            out.writeString(splitBrainProtectionName);
            out.writeObject(mergePolicyConfig);
            out.writeObject(userCodeNamespace);
        }
        SerializationUtil.writeList(vectorIndexConfigs, out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        vectorIndexConfigs.clear();
        name = in.readString();
        // RU_COMPAT_5_5
        if (in.getVersion().isGreaterOrEqual(V6_0)) {
            backupCount = in.readInt();
            asyncBackupCount = in.readInt();
            splitBrainProtectionName = in.readString();
            mergePolicyConfig = in.readObject();
            userCodeNamespace = in.readObject();
        } else {
            // in 5.5 there were no backups, override new defaults to keep original behavior
            backupCount = 0;
            asyncBackupCount = 0;
        }
        List<VectorIndexConfig> deserialized = SerializationUtil.readList(in);
        vectorIndexConfigs.addAll(deserialized);
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.VECTOR_COLLECTION_CONFIG;
    }

    @Override
    public String toString() {
        return "VectorCollectionConfig{"
                + "name='" + name + '\''
                + ", backupCount=" + backupCount
                + ", asyncBackupCount=" + asyncBackupCount
                + ", splitBrainProtectionName=" + splitBrainProtectionName
                + ", mergePolicyConfig=" + mergePolicyConfig
                + ", userCodeNamespace=" + userCodeNamespace
                + ", vectorIndexConfigs=" + vectorIndexConfigs
                + '}';
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        VectorCollectionConfig that = (VectorCollectionConfig) object;
        return Objects.equals(name, that.name)
                && Objects.equals(backupCount, that.backupCount)
                && Objects.equals(asyncBackupCount, that.asyncBackupCount)
                && Objects.equals(splitBrainProtectionName, that.splitBrainProtectionName)
                && Objects.equals(mergePolicyConfig, that.mergePolicyConfig)
                && Objects.equals(userCodeNamespace, that.userCodeNamespace)
                && Objects.equals(vectorIndexConfigs, that.vectorIndexConfigs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                name,
                backupCount,
                asyncBackupCount,
                splitBrainProtectionName,
                mergePolicyConfig,
                userCodeNamespace,
                vectorIndexConfigs
        );
    }

    private static void validateName(String name) {
        String allowedSymbols = "[a-zA-Z0-9\\-*_]+";
        if (!name.matches(allowedSymbols)) {
            throw new IllegalArgumentException("The name of the vector collection "
                    + "should only consist of letters, numbers, and the symbols \"-\", \"_\" or \"*\".");
        }
    }

    private void validateIndexConfig(List<VectorIndexConfig> newIndexConfig) {
        Set<String> seenNames = new HashSet<>();

        for (var index : newIndexConfig) {
            if (!seenNames.add(index.getName())) {
                throw new InvalidConfigurationException(
                        "The vector index configuration contains multiple indexes with the same name: " + index.getName()
                );
            }
        }

        if (newIndexConfig.size() > 1 && newIndexConfig.stream().anyMatch(index -> index.getName() == null)) {
            throw new InvalidConfigurationException("Vector collection cannot contain both named and unnamed index");
        }
    }

    /**
     *
     * @since 6.0
     */
    @Override
    @Nullable
    public String getUserCodeNamespace() {
        return userCodeNamespace;
    }

    /**
     *
     * @since 6.0
     */
    @Override
    public VectorCollectionConfig setUserCodeNamespace(@Nullable String userCodeNamespace) {
        this.userCodeNamespace = userCodeNamespace;
        return this;
    }
}
