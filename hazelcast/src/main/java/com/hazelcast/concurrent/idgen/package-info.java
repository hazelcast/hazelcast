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

/**
 * <p>This package contains IdGenerator functionality for Hazelcast.<br/>
 * With the {@link com.hazelcast.core.IdGenerator} it is very simple to create cluster wide id's.
 * This can also be done with the {@link com.hazelcast.core.IAtomicLong}, but this would require
 * access to the cluster for every id generated. With the IdGenerator this is a lot more efficient
 * by claiming a whole check and only when the chunk is depleted, cluster access is needed.
 *
 * @since 2
 */
package com.hazelcast.concurrent.idgen;
