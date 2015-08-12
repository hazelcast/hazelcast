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

package com.hazelcast.internal.metrics;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Annotation to be put on a field to indicate that this field contains probes (directly or indirectly).
 *
 * If a probe is put on a:
 * <ol>
 *     <li>map: then the values will be inspected</li>
 *     <li>collection: then the items wll be inspected</li>
 *     <li>array: the items will be inspected</li>
 *     <li>regular object: its fields will be inspected</li>
 * </ol>
 *
 * ContainsProbes annotations makes it possible to have 'dynamic' probes. So a root object like a TcpIpConnectionManager
 * is registered and on the set of active connections a @ContainsProbe annotation is placed. This way the MetricsRegistry
 * knows it can dive into these connections for additional probes.
 */
@Retention(value = RUNTIME)
@Target({FIELD })
public @interface ContainsProbes {
}
