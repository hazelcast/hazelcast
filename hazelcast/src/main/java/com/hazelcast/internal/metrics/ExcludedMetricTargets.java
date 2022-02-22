/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.metrics;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Annotation for defining class-level {@link MetricTarget} exclusions.
 * The final excluded targets for every metric from the class annotated
 * with this annotation will be the union of the exclusions defined with
 * this annotation and the exclusions defined on the {@link Probe}s.
 *
 * @see Probe#excludedTargets()
 */
@Retention(RUNTIME)
@Target(TYPE)
public @interface ExcludedMetricTargets {

    /**
     * Returns the targets excluded for the class annotated with this
     * annotation. Used for filtering in {@link MetricsPublisher}s.
     *
     * @return the targets excluded for the class
     */
    MetricTarget[] value();
}
