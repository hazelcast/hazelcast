/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.operation;

/**
 * Interface for operations that may require execution on the master node.
 * <p>
 * If {@link #isRequireMasterExecution()} is true, the operation
 * should validate that it is being executed on the current master node before proceeding.
 * <p>
 * The actual validation logic must be implemented by the operation itself.
 */
public interface MasterAwareOperation {
    boolean isRequireMasterExecution();
}
