/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.blackbox.impl;

import com.hazelcast.internal.blackbox.Blackbox;
import com.hazelcast.internal.blackbox.DoubleSensorInput;
import com.hazelcast.internal.blackbox.LongSensorInput;
import com.hazelcast.internal.blackbox.Sensor;
import com.hazelcast.internal.blackbox.sensorpacks.ClassLoadingSensorPack;
import com.hazelcast.internal.blackbox.sensorpacks.GarbageCollectionSensorPack;
import com.hazelcast.internal.blackbox.sensorpacks.OperatingSystemSensorPack;
import com.hazelcast.internal.blackbox.sensorpacks.RuntimeSensorPack;
import com.hazelcast.internal.blackbox.sensorpacks.ThreadSensorPack;
import com.hazelcast.logging.ILogger;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.lang.String.format;

/**
 * The {@link Blackbox} implementation.
 *
 * Interesting read:
 * https://source.android.com/devices/sensors/sensor-types.html
 */
public class BlackboxImpl implements Blackbox {

    private final ILogger logger;
    private final ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(2);
    private final AtomicInteger modCount = new AtomicInteger();
    private final ConcurrentMap<String, SensorImpl> sensors = new ConcurrentHashMap<String, SensorImpl>();
    private final ConcurrentMap<Class<?>, SourceMetadata> metadataMap
            = new ConcurrentHashMap<Class<?>, SourceMetadata>();

    /**
     * Creates a BlackboxImpl instance.
     *
     * Automatically registers the com.hazelcast.internal.blackbox.sensorpacks.
     *
     * @param logger the ILogger used
     * @throws NullPointerException if logger is null
     */
    public BlackboxImpl(ILogger logger) {
        this.logger = checkNotNull(logger, "Logger can't be null");

        RuntimeSensorPack.register(this);
        GarbageCollectionSensorPack.register(this);
        OperatingSystemSensorPack.register(this);
        ThreadSensorPack.register(this);
        ClassLoadingSensorPack.register(this);
    }

    @Override
    public int modCount() {
        return modCount.get();
    }

    @Override
    public Set<String> getParameters() {
        return sensors.keySet();
    }

    SourceMetadata getObjectMetadata(Class<?> clazz) {
        SourceMetadata metadata = metadataMap.get(clazz);
        if (metadata == null) {
            metadata = new SourceMetadata(clazz);
            SourceMetadata found = metadataMap.putIfAbsent(clazz, metadata);
            metadata = found == null ? metadata : found;
        }

        return metadata;
    }

    @Override
    public synchronized <S> void scanAndRegister(S source, String parameterPrefix) {
        checkNotNull(source, "source can't be null");
        checkNotNull(parameterPrefix, "parameterPrefix can't be null");

        SourceMetadata metadata = getObjectMetadata(source.getClass());
        metadata.register(this, source, parameterPrefix);
    }

    @Override
    public synchronized <S> void register(S source, String parameter, LongSensorInput<S> input) {
        checkNotNull(parameter, "source can't be null");
        checkNotNull(parameter, "parameter can't be null");
        checkNotNull(parameter, "input can't be null");

        registerInternal(source, parameter, input);
    }

    @Override
    public synchronized <S> void register(S source, String parameter, DoubleSensorInput<S> input) {
        checkNotNull(parameter, "source can't be null");
        checkNotNull(parameter, "parameter can't be null");
        checkNotNull(parameter, "input can't be null");

        registerInternal(source, parameter, input);
    }

    <S> void registerInternal(S source, String parameter, Object input) {
        SensorImpl sensor = sensors.get(parameter);
        if (sensor == null) {
            sensor = new SensorImpl<S>(parameter, logger);
            sensors.put(parameter, sensor);
        }

        logOverwrite(parameter, sensor);

        if (logger.isFinestEnabled()) {
            logger.finest("Registered sensor " + parameter);
        }

        sensor.source = source;
        sensor.input = input;

        modCount.incrementAndGet();
    }

    /**
     * We are going to check if a source or input already exist. If it exists, we are going the log a warning but we
     * are not going to fail. The blackbox should never fail unless the arguments don't make any sense of course.
     */
    private void logOverwrite(String parameter, SensorImpl sensor) {
        // if an input already exists, we are just going to overwrite it.
        if (sensor.input != null) {
            logger.warning(format("Duplicate registration, a SensorInput for Sensor '%s' already exists", parameter));
        }

        // if a source already exists, we are just going to overwrite it.
        if (sensor.source != null) {
            logger.warning(format("Duplicate registration, a source for Sensor '%s' already exists", parameter));
        }
    }

    @Override
    public synchronized Sensor getSensor(String parameter) {
        checkNotNull(parameter, "parameter can't be null");

        SensorImpl sensor = sensors.get(parameter);

        if (sensor == null) {
            sensor = new SensorImpl(parameter, logger);
            sensors.put(parameter, sensor);
        }

        return sensor;
    }

    @Override
    public synchronized <S> void deregister(S source) {
        checkNotNull(source, "source can't be null");

        boolean changed = false;
        for (Map.Entry<String, SensorImpl> entry : sensors.entrySet()) {
            SensorImpl sensor = entry.getValue();
            if (sensor.source != source) {
                continue;
            }

            String parameter = entry.getKey();
            changed = true;
            sensors.remove(parameter);
            sensor.source = null;
            sensor.input = null;

            if (logger.isFinestEnabled()) {
                logger.finest("Destroying sensor " + parameter);
            }
        }

        if (changed) {
            modCount.incrementAndGet();
        }
    }

    @Override
    public void scheduleAtFixedRate(final Runnable publisher, long period, TimeUnit timeUnit) {
        scheduledExecutorService.scheduleAtFixedRate(publisher, 0, period, timeUnit);
    }

    public void shutdown() {
        scheduledExecutorService.shutdown();
    }
}
