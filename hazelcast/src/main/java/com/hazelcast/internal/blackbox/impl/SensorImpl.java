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

import com.hazelcast.internal.blackbox.DoubleSensorInput;
import com.hazelcast.internal.blackbox.LongSensorInput;
import com.hazelcast.internal.blackbox.Sensor;
import com.hazelcast.logging.ILogger;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Default {@link Sensor} implementation.
 *
 * @param <S>
 */
public class SensorImpl<S> implements Sensor {

    volatile Object input;
    volatile S source;

    private final ILogger logger;
    private final String parameter;

    public SensorImpl(String parameter, ILogger logger) {
        this.parameter = parameter;
        this.logger = logger;
    }

    @Override
    public String getParameter() {
        return parameter;
    }

    @Override
    public void render(StringBuilder sb) {
        checkNotNull(sb, "sb can't be null");

        sb.append(parameter).append('=');

        Object input = this.input;
        S source = this.source;

        if (input == null || source == null) {
            sb.append("NA");
            return;
        }

        try {
            if (input instanceof LongSensorInput) {
                LongSensorInput function = (LongSensorInput) input;
                sb.append(function.get(source));
            } else if (input instanceof DoubleSensorInput) {
                DoubleSensorInput function = (DoubleSensorInput) input;
                sb.append(function.get(source));
            } else if (input instanceof AccessibleObjectInput) {
                AccessibleObjectInput accessibleObjectInput = (AccessibleObjectInput) input;
                if (accessibleObjectInput.isDouble()) {
                    sb.append(accessibleObjectInput.getDouble(source));
                } else {
                    sb.append(accessibleObjectInput.getLong(source));
                }
            }
        } catch (Exception e) {
            logger.warning("Failed to update sensor:" + parameter, e);
            sb.append("NA");
        }
    }

    @Override
    public long readLong() {
        Object input = this.input;
        S source = this.source;
        long result = 0;

        if (input == null || source == null) {
            return result;
        }

        try {
            if (input instanceof AccessibleObjectInput) {
                AccessibleObjectInput accessibleObjectInput = (AccessibleObjectInput) input;
                if (accessibleObjectInput.isDouble()) {
                    double doubleResult = accessibleObjectInput.getDouble(source);
                    result = Math.round(doubleResult);
                } else {
                    result = accessibleObjectInput.getLong(source);
                }
            } else if (input instanceof LongSensorInput) {
                LongSensorInput<S> longInput = (LongSensorInput<S>) input;
                result = longInput.get(source);
            } else if (input instanceof DoubleSensorInput) {
                DoubleSensorInput<S> doubleInput = (DoubleSensorInput<S>) input;
                double doubleResult = doubleInput.get(source);
                result = Math.round(doubleResult);
            }
            return result;
        } catch (Exception e) {
            logger.warning("Failed to update sensor:" + parameter, e);
            return result;
        }
    }

    @Override
    public double readDouble() {
        Object input = this.input;
        S source = this.source;
        double result = 0;

        if (input == null || source == null) {
            return result;
        }

        try {
            if (input instanceof AccessibleObjectInput) {
                AccessibleObjectInput accessibleObjectInput = (AccessibleObjectInput) input;
                if (accessibleObjectInput.isDouble()) {
                    result = accessibleObjectInput.getDouble(source);
                } else {
                    result = accessibleObjectInput.getLong(source);
                }
            } else if (input instanceof LongSensorInput) {
                LongSensorInput<S> longInput = (LongSensorInput<S>) input;
                result = longInput.get(source);
            } else {
                DoubleSensorInput<S> doubleInput = (DoubleSensorInput<S>) input;
                result = doubleInput.get(source);
            }
            return result;
        } catch (Exception e) {
            logger.warning("Failed to update sensor:" + parameter, e);
            return result;
        }
    }
}
