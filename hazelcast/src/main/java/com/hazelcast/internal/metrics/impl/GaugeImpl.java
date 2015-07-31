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

package com.hazelcast.internal.metrics.impl;

import com.hazelcast.internal.metrics.DoubleProbe;
import com.hazelcast.internal.metrics.Gauge;
import com.hazelcast.internal.metrics.LongProbe;
import com.hazelcast.internal.metrics.Metric;
import com.hazelcast.logging.ILogger;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Default {@link Metric} implementation.
 *
 * @param <S>
 */
public class GaugeImpl<S> implements Gauge {

    volatile Object input;
    volatile S source;

    private final ILogger logger;
    private final String name;

    public GaugeImpl(String name, ILogger logger) {
        this.name = name;
        this.logger = logger;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void render(StringBuilder sb) {
        checkNotNull(sb, "sb can't be null");

        sb.append(name).append('=');

        Object input = this.input;
        S source = this.source;

        if (input == null || source == null) {
            sb.append("NA");
            return;
        }

        try {
            if (input instanceof LongProbe) {
                LongProbe<S> function = (LongProbe) input;
                sb.append(function.get(source));
            } else {
                DoubleProbe<S> function = (DoubleProbe) input;
                sb.append(function.get(source));
            }
        } catch (Exception e) {
            logger.warning("Failed to update metric:" + name, e);
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
            if (input instanceof LongProbe) {
                LongProbe<S> longInput = (LongProbe<S>) input;
                result = longInput.get(source);
            } else {
                DoubleProbe<S> doubleInput = (DoubleProbe<S>) input;
                double doubleResult = doubleInput.get(source);
                result = Math.round(doubleResult);
            }
            return result;
        } catch (Exception e) {
            logger.warning("Failed to update metric:" + name, e);
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
            if (input instanceof LongProbe) {
                LongProbe<S> longInput = (LongProbe<S>) input;
                result = longInput.get(source);
            } else {
                DoubleProbe<S> doubleInput = (DoubleProbe<S>) input;
                result = doubleInput.get(source);
            }
            return result;
        } catch (Exception e) {
            logger.warning("Failed to update metric:" + name, e);
            return result;
        }
    }
}
