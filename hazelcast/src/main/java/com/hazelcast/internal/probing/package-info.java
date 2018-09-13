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

/**
 * Contains the Hazelcast probing facility.
 *
 * A client or node will create a
 * {@link com.hazelcast.internal.probing.ProbeRegistry}. During startup the
 * central services and managers are registered as
 * {@link com.hazelcast.internal.probing.ProbeRegistry.com.hazelcast.internal.probing.ProbeRegistry.ProbeSource}.
 * These are the root objects that are the starting points of running
 * {@link com.hazelcast.internal.probing.ProbingCycle}s to render the measured
 * data using a {@link com.hazelcast.internal.probing.ProbeRenderer}.
 */
package com.hazelcast.internal.probing;
