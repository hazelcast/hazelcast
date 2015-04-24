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

/**
 * A {@link Sensor} provides access to measure long/double value.
 *
 * If the Sensor is obtained before it is registered, the Sensor is without input. As soon as the registration happens, the
 * input is set on the sensor and the current information will be retrieved.
 *
 * A Sensor can be used before it is registered and there has no input/source set. In this case the {@link #readDouble()} and
 * {@link #readDouble()} return 0.
 *
 * A Sensor can be used after the {@link Blackbox#deregister(Object)} is called. In this case the {@link #readDouble()} and
 * {@link #readDouble()} return 0.
 *
 * Some future improvements:
 * <ol>
 * <li>return type type, floating point or not</li>
 * <li>provide insight if the source is set</li>
 * </ol>
 * For the time being they are not needed, so therefor not implemented.
 */
public interface Sensor {

    /**
     * Reads the current available value as a long.
     *
     * If the underlying sensor input providing a floating point value, then the value will be rounded using
     * {@link Math#round(double)}.
     *
     * If no input is available, or there a problems obtaining a value from the input, 0 is returned.
     *
     * @return the current value.
     */
    long readLong();

    /**
     * Reads the current available value as a double.
     *
     * If the underlying sensor input doesn't provide a floating point value, then the value will be converted to
     * a floating point value.
     *
     * If no input is available, or there a problems obtaining a value from the input, 0 is returned.
     *
     * @return the current value.
     */
    double readDouble();

    /**
     * Gets the parameter that identifies this sensor. The returned value will never change and never be null.
     *
     * @return the parameter.
     */
    String getParameter();

    /**
     * Renders the Sensor.
     *
     * @param sb the StringBuilder to write to.
     * @throws NullPointerException if sb is null
     */
    void render(StringBuilder sb);
}
