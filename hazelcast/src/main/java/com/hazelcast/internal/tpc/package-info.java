/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
 * This package contains TPC integration for Hazelcast.
 * <p/>
 * The com.hazelcast.internal.tpcengine package contains the lower level
 * {@link com.hazelcast.internal.tpcengine.TpcEngine}. This engine is useful
 * for asynchronous I/O and processing. It is unaware of Hazelcast.
 * <p/>
 * The com.hazelcast.internal.tpc package contains the integration of Hazelcast
 * on top of the {@link com.hazelcast.internal.tpcengine.TpcEngine}. This is
 * where we'll see Hazelcast specific functionality.
 */
package com.hazelcast.internal.tpc;
