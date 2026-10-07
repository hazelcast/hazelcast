/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.cpmap.operation;

import com.hazelcast.internal.serialization.Data;

/**
 * Factory for CPMap operations.
 * <p>
 * Provides a single, centralized place to create CPMap
 * operations for both client and server-side code paths.
 */
public interface CPMapOperationProvider {

    CPMapSetOp newCpMapSetOp(String objectName, Data key, Data value);

    CPMapPutOp newCpMapPutOp(String objectName, Data key, Data value);

    CPMapPutIfAbsentOp newCpMapPutIfAbsentOp(String objectName, Data key, Data value);

    CPMapCompareAndSetOp newCompareAndSetOp(
            String objectName,
            Data key,
            Data expectedValue,
            Data newValue
    );
}
