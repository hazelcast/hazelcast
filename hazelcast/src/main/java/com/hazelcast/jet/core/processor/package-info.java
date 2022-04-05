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

/**
 * Contains static utility classes with factories of Jet processors. The
 * factories are organized into several categories according to the role
 * of the vertex they implement:
 * <ul><li>
 *     {@link com.hazelcast.jet.core.processor.SourceProcessors} (have no inbound edges)
 * </li><li>
 *     {@link com.hazelcast.jet.core.processor.SinkProcessors} (have no outbound edges)
 * </li><li>
 *     {@link com.hazelcast.jet.core.processor.Processors Internal vertices}
 *     (have both inbound and outbound edges)
 * </li><li>
 *     {@link com.hazelcast.jet.core.processor.DiagnosticProcessors Diagnostic
 *     helpers}
 * </li></ul>
 *
 * @since Jet 3.0
 */
package com.hazelcast.jet.core.processor;
