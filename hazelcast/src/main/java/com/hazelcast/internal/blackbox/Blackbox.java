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

package com.hazelcast.internal.blackbox;

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * The Blackbox is responsible for recording all kinds of Hazelcast/JVM specific information to
 * help out with all kinds of issues.
 *
 * Each HazelcastInstance has its own Blackbox.
 *
 * A Blackbox can contain many {@link Sensor}. Each sensor has an input and each sensor can be identified using a
 * parameter; a String. This parameter can be any string, the general structure is something like:
 * <ol>
 *     <li>proxy.count</li>
 *     <li>operation.completed.count</li>
 *     <li>operation.partition[14].count</li>
 * </ol>
 * For the time being there the Blackbox doesn't require any syntax for the parameter content; so any String is fine.
 *
 * <h1>Duplicate Registrations</h1>
 * The Blackbox is also lenient regarding duplicate registrations of sensors. So if a Sensor is created and
 * an SensorInput is set and a new registration for the same Sensor is done, the old SensorInput is overwritten.
 * The reason to be lenient is that the Blackbox should not throw exception. Of course there will be a log
 * warning.
 *
 * <h1>Performance</h1>
 * The Blackbox is designed for low overhead sensors. So once a sensor is registered, there is no overhead
 * for the provider of the sensor data. The provider could have for example a volatile long field and increment
 * this using a lazy-set. As long as the Blackbox can frequently read out this field, the Blackbox is perfectly
 * happy with such low overhead sensor-inputs. So it is up to the provider of the sensor-input how much overhead
 * is required.
 */
public interface Blackbox {

    /**
     * Scans the source object for any fields/methods that have been annotated with {@link SensorInput} annotation, and
     * registering these fields/methods as sensors.
     *
     * If sensors with the same parameter already exist, there source/inputs will be updated. So multiple registrations
     * if the same object are ignored.
     *
     * If an object has no sensor annotations, the call is ignored.
     *
     * @param source          the object to scan.
     * @param parameterPrefix the parameter prefix.
     * @throws NullPointerException     if parameterPrefix or source is null.
     * @throws IllegalArgumentException if the source contains SensorInput annotation on a field/method of unsupported type.
     */
    <S> void scanAndRegister(S source, String parameterPrefix);

    /**
     * Registers a sensor.
     *
     * If a Sensor with the given name already has a SensorInput, that SensorInput will be overwritten.
     *
     * @param parameter the name of the sensor.
     * @param input     the input for the sensor.
     * @throws NullPointerException if source, parameter or input is null.
     */
    <S> void register(S source, String parameter, LongSensorInput<S> input);

    /**
     * Registers a sensor.
     *
     * If a Sensor with the given name already has a SensorInput, that SensorInput will be overwritten.
     *
     * @param parameter the name of the sensor.
     * @param input     the input for the sensor.
     * @throws NullPointerException if parameter or input is null.
     */
    <S> void register(S source, String parameter, DoubleSensorInput<S> input);


    /**
     * Deregisters an scanned object. All sensors that were for this given source object are removed.
     *
     * If the object already is deregistered, the call is ignored.
     *
     * If the object was never registered, the call is ignored.
     *
     * @param source the object to deregister
     * @throws NullPointerException if source is null.
     */
    <S> void deregister(S source);

    /**
     * Schedules a publisher to be executed at a fixed rate.
     *
     * Probably this method will be removed in the future, but we need a mechanism for complex sensors that require some
     * calculation to provide their values.
     *
     * @param publisher the published task that needs to be executed
     * @param period    the time between executions
     * @param timeUnit  the timeunit for period
     * @throws NullPointerException if publisher or timeUnit is null.
     */
    void scheduleAtFixedRate(Runnable publisher, long period, TimeUnit timeUnit);

    /**
     * Gets the Sensor for a given parameter.
     *
     * If no sensor exists for the parameter, it will be created but no SensorInput is set. The reason to do so is
     * that you don't want to depend on the order of registration. Perhaps you want to read out e.g. operations.count
     * sensor, but the OperationService has not started yet and the sensor is not yet available.
     *
     * @param parameter the parameter
     * @return the Sensor. Multiple calls with the same parameter, return the same Sensor instance.
     * @throws NullPointerException if parameter is null.
     */
    Sensor getSensor(String parameter);

    /**
     * Gets a set of all current parameters.
     *
     * @return set of all current parameters.
     */
    Set<String> getParameters();

    /**
     * Returns the modCount. Every time a sensor is added or removed, the modCount is increased.
     *
     * Returned modcount will always be equal or larger than 0.
     *
     * @return the modCount.
     */
    int modCount();
}
