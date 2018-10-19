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

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import com.hazelcast.internal.metrics.CollectionCycle.Tags;

/**
 * Used to annotate type with {@link Probe} annotated fields or methods to give
 * all of them a common name-space tag.
 *
 * This has a similar effect to implementing
 * {@link MetricsNs#switchContext(com.hazelcast.internal.metrics.CollectionCycle.Tags)}
 * with {@link Tags#namespace(CharSequence)} called with the value returned by
 * {@link #value()}.
 */
@Retention(RUNTIME)
@Target(TYPE)
public @interface Namespace {

    /**
     * @return name of the namespace
     */
    String value();
}
