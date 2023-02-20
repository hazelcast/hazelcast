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

package com.hazelcast.internal.tpc;

import java.util.function.Supplier;

/**
 * Needs to be implemented by threads that are passed using the
 * {@link ReactorBuilder#setThreadSupplier(Supplier)}. Such threads
 * can start early, but need to wait till the eventloop task is set.
 * <p/>
 * The primary reason this interface exists, is for compatibility with the
 * AltoPartitionOperationThread.
 */
public interface SuppliedThread {
    void setEventloopTask(Runnable eventloopTask);
}
