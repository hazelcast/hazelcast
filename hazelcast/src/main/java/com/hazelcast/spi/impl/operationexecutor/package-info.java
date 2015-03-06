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
 * Contains the logic for executing/running Operations.
 *
 * This logic has been pulled out of the {@link com.hazelcast.spi.OperationService} so you get a separation of concerns. The
 * actual processing can be done there, while the assignment of an operation to a thread is the responsibility of the
 * {@link com.hazelcast.spi.impl.operationexecutor.OperationExecutor}.
 */
package com.hazelcast.spi.impl.operationexecutor;
