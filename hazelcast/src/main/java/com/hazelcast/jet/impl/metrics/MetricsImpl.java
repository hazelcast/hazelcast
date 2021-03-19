/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.metrics;

import com.hazelcast.jet.core.metrics.Metric;
import com.hazelcast.jet.core.metrics.Unit;

import javax.annotation.Nullable;

public final class MetricsImpl {

    private static final ThreadLocal<Container> CONTEXT = ThreadLocal.withInitial(Container::new);

    private MetricsImpl() {
    }

    public static Container container() {
        return CONTEXT.get();
    }

    public static Metric metric(String name, Unit unit) {
        return getContext().metric(name, unit);
    }

    public static Metric threadSafeMetric(String name, Unit unit) {
        return getContext().threadSafeMetric(name, unit);
    }

    private static MetricsContext getContext() {
        Container container = CONTEXT.get();
        MetricsContext context = container.getContext();
        if (context == null) {
            throw new RuntimeException("Thread %s has no metrics context set, this method can " +
                    "be called only on threads executing the job's processors");
        }
        return context;
    }

    public static class Container {

        @Nullable
        private MetricsContext context;

        Container() {
        }

        @Nullable
        public MetricsContext getContext() {
            return context;
        }

        public void setContext(@Nullable MetricsContext context) {
            this.context = context;
        }
    }

}
