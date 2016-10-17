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

package com.hazelcast.spi;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.quorum.QuorumService;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.transaction.TransactionManagerService;
import com.hazelcast.wan.WanReplicationService;

/**
 * The NodeEngine is the 'umbrella' of services/service-method that gets injected into a {@link ManagedService}.
 * <p/>
 * So if you are writing a custom SPI service, such as a stack-service, this service should probably implement
 * the {@link ManagedService} so you can get access to the services within the system.
 */
public interface NodeEngine {

    /**
     * Gets the OperationService.
     *
     * @return the OperationService.
     */
    OperationService getOperationService();

    /**
     * Gets the ExecutionService.
     *
     * @return the ExecutionService.
     */
    ExecutionService getExecutionService();

    /**
     * Gets the ClusterService.
     *
     * @return the ClusterService.
     */
    ClusterService getClusterService();

    /**
     * Gets the IPartitionService.
     *
     * @return the IPartitionService.
     */
    IPartitionService getPartitionService();

    /**
     * Gets the EventService.
     *
     * @return the EventService.
     */
    EventService getEventService();

    /**
     * Gets the SerializationService.
     *
     * @return the SerializationService.
     */
    SerializationService getSerializationService();

    /**
     * Gets the ProxyService.
     *
     * @return the ProxyService.
     */
    ProxyService getProxyService();

    /**
     * Gets the WanReplicationService.
     *
     * @return the WanReplicationService.
     */
    WanReplicationService getWanReplicationService();

    /**
     * Gets the QuorumService.
     *
     * @return the QuorumService.
     */
    QuorumService getQuorumService();

    /**
     * Gets the TransactionManagerService.
     *
     * @return the TransactionManagerService.
     */
    TransactionManagerService getTransactionManagerService();

    /**
     * Gets the address of the master member.
     * <p/>
     * This value can be null if no master is elected yet.
     * <p/>
     * The value can change over time.
     *
     * @return the address of the master member.
     */
    Address getMasterAddress();

    /**
     * Get the address of this member.
     * <p/>
     * The returned value will never change and will never be null.
     *
     * @return the address of this member.
     */
    Address getThisAddress();

    /**
     * Returns the local member.
     * <p/>
     * The returned value will never change and will never be null.
     *
     * @return the local member.
     */
    Member getLocalMember();

    /**
     * Returns the Config that was used to create the HazelcastInstance.
     * <p/>
     * The returned value will never change and will never be null.
     *
     * @return the config.
     */
    Config getConfig();

    /**
     * Returns the Config ClassLoader.
     * <p/>
     * todo: add more documentation what the purpose is of the config classloader.
     *
     * @return the config ClassLoader.
     */
    ClassLoader getConfigClassLoader();

    /**
     * Returns the HazelcastProperties.
     * <p/>
     * The returned value will never change and will never be null.
     *
     * @return the HazelcastProperties
     */
    HazelcastProperties getProperties();

    /**
     * Gets the logger for a given name.
     * <p/>
     * It is best to get an ILogger through this method instead of doing
     *
     * @param name the name of the logger.
     * @return the ILogger.
     * @throws NullPointerException if name is null.
     * @see #getLogger(Class)
     */
    ILogger getLogger(String name);

    /**
     * Gets the logger for a given class.
     *
     * @param clazz the class of the logger.
     * @return the ILogger.
     * @throws NullPointerException if clazz is null.
     * @see #getLogger(String)
     */
    ILogger getLogger(Class clazz);

    /**
     * Serializes an object to a {@link Data}.
     * <p/>
     * This method can safely be called with a {@link Data} instance. In that case, that instance is returned.
     * <p/>
     * If this method is called with null, null is returned.
     *
     * @param object the object to serialize.
     * @return the serialized object.
     * @throws com.hazelcast.nio.serialization.HazelcastSerializationException when serialization fails.
     */
    Data toData(Object object);

    /**
     * Deserializes an object.
     * <p/>
     * This method can safely be called on an object that is already deserialized. In that case, that instance
     * is returned.
     * <p/>
     * If this method is called with null, null is returned.
     *
     * @param object the object to deserialize.
     * @return the deserialized object.
     * @throws com.hazelcast.nio.serialization.HazelcastSerializationException when deserialization fails.
     */
    <T> T toObject(Object object);

    /**
     * Deserializes an object.
     * <p/>
     * This method can safely be called on an object that is already deserialized. In that case, that instance
     * is returned.
     * <p/>
     * If this method is called with null, null is returned.
     *
     * @param object the object to deserialize.
     * @param klazz The class to instantiate when deserializing the object.
     * @return the deserialized object.
     * @throws com.hazelcast.nio.serialization.HazelcastSerializationException when deserialization fails.
     */
    <T> T toObject(Object object, Class klazz);

    /**
     * Checks if the HazelcastInstance that this {@link NodeEngine} belongs to is still active.
     * <p/>
     * A HazelcastInstance is not active when it is shutting down or already shut down.
     * * Also see {@link NodeEngine#isRunning()}
     *
     * @return true if active, false otherwise.
     */
    @Deprecated
    boolean isActive();

    /**
     * Indicates that node is not shutting down or it has not already shut down
     *
     * @return true if node is not shutting down or it has not already shut down
     */
    boolean isRunning();

    /**
     * Returns the HazelcastInstance that this {@link NodeEngine} belongs to.
     *
     * @return the HazelcastInstance
     */
    HazelcastInstance getHazelcastInstance();

    /**
     * Gets the service with the given name.
     *
     * @param serviceName the name of the service
     * @param <T> the type of the service.
     * @return the found service, or HazelcastException in case of failure. Null will not be returned.
     */
    <T> T getService(String serviceName);

    /**
     * Gets the {@link SharedService} for the given serviceName.
     *
     * @param serviceName the name of the shared service to get.
     * @param <T>
     * @return the found service, or null if the service was not found.
     * @throws NullPointerException if the serviceName is null.
     * @deprecated since 3.7. Use {@link #getService(String)} instead.
     */
    <T extends SharedService> T getSharedService(String serviceName);
}
