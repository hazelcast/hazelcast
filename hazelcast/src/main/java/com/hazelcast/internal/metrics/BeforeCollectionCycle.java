/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

/**
 * Marks the method of a {@link MetricsSource} that should be called when it is
 * time to update the internal state by calling the annotated method.
 *
 * This can be used as an alternative when computing current values for or
 * during each {@link CollectionCycle} is too "expensive". Updating instead is
 * issued by calling the annotated method before data collection when at least
 * the the specified amount of time has passed since the last update.
 */
@Retention(RUNTIME)
@Target(METHOD)
public @interface BeforeCollectionCycle {

    int value() default 1;

    TimeUnit unit() default TimeUnit.SECONDS;

    /**
     * By setting a {@link ProbeLevel} with less precedence this the update can
     * effectively be disabled when collection is on a higher precedence level.
     *
     * @return the minimum level for which the update should occur
     */
    ProbeLevel level() default ProbeLevel.MANDATORY;
}
