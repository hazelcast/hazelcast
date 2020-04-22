/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.sql.impl.optimizer.NoOpSqlOptimizer;
import com.hazelcast.sql.impl.optimizer.SqlOptimizer;

import java.lang.reflect.Constructor;
import java.util.function.Consumer;

/**
 * Base SQL service implementation that bridges optimizer implementation, public and private APIs.
 */
public class SqlServiceImpl implements Consumer<Packet> {
    /** Outbox batch size in bytes. */
    private static final int OUTBOX_BATCH_SIZE = 512 * 1024;

    /** Default state check frequency. */
    private static final long STATE_CHECK_FREQUENCY = 10_000L;

    private static final String OPTIMIZER_CLASS_PROPERTY_NAME = "hazelcast.sql.optimizerClass";
    private static final String OPTIMIZER_CLASS_DEFAULT = "com.hazelcast.sql.impl.calcite.CalciteSqlOptimizer";

    private final SqlOptimizer optimizer;

    private volatile SqlInternalService internalService;

    public SqlServiceImpl(NodeEngineImpl nodeEngine) {
        // These two parameters will be taken from the config, when public API is ready.
        int operationThreadCount = Runtime.getRuntime().availableProcessors();
        int fragmentThreadCount = Runtime.getRuntime().availableProcessors();

        NodeServiceProviderImpl nodeServiceProvider = new NodeServiceProviderImpl(nodeEngine);

        String instanceName = nodeEngine.getHazelcastInstance().getName();
        InternalSerializationService serializationService = (InternalSerializationService) nodeEngine.getSerializationService();

        internalService = new SqlInternalService(
            instanceName,
            nodeServiceProvider,
            serializationService,
            operationThreadCount,
            fragmentThreadCount,
            OUTBOX_BATCH_SIZE,
            STATE_CHECK_FREQUENCY
        );

        optimizer = createOptimizer(nodeEngine);
    }

    public void start() {
        internalService.start();
    }

    public void reset() {
        internalService.reset();
    }

    public void shutdown() {
        internalService.shutdown();
    }

    public SqlInternalService getInternalService() {
        return internalService;
    }

    /**
     * For testing only.
     */
    public void setInternalService(SqlInternalService internalService) {
        this.internalService = internalService;
    }

    public SqlOptimizer getOptimizer() {
        return optimizer;
    }

    @Override
    public void accept(Packet packet) {
        internalService.onPacket(packet);
    }

    /**
     * Create either normal or no-op optimizer instance.
     *
     * @param nodeEngine Node engine.
     * @return Optimizer.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static SqlOptimizer createOptimizer(NodeEngine nodeEngine) {
        // 1. Resolve class name.
        String className = System.getProperty(OPTIMIZER_CLASS_PROPERTY_NAME, OPTIMIZER_CLASS_DEFAULT);

        // 2. Get the class.
        Class clazz;

        try {
            clazz = Class.forName(className);
        } catch (ClassNotFoundException e) {
            return new NoOpSqlOptimizer();
        } catch (Exception e) {
            throw new HazelcastException("Failed to resolve optimizer class " + className + ": " + e.getMessage(), e);
        }

        // 3. Get required constructor.
        Constructor<SqlOptimizer> constructor;

        try {
            constructor = clazz.getConstructor(NodeEngine.class);
        } catch (ReflectiveOperationException e) {
            throw new HazelcastException("Failed to get the constructor for the optimizer class "
                + className + ": " + e.getMessage(), e);
        }

        // 4. Finally, get the instance.
        try {
            return constructor.newInstance(nodeEngine);
        } catch (ReflectiveOperationException e) {
            throw new HazelcastException("Failed to instantiate the optimizer class " + className + ": " + e.getMessage(), e);
        }
    }
}
