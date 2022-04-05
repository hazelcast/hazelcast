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

package com.hazelcast.spi.exception;

import com.hazelcast.spi.impl.operationservice.Operation;

/**
 * Marker interface for exceptions.
 *
 * When an exception is marked with this interface then
 * it won't be logged by {@link Operation#logError(Throwable)} on the
 * callee side.
 *
 * It's intended to be used for exceptions which are part of a flow,
 * for example {@link com.hazelcast.durableexecutor.StaleTaskIdException}
 * is always propagated to the user - there is no reason why Hazelcast
 * should log it on its own.
 *
 * The exception is silent from Hazelcast point of view only. Obviously
 * it's very visible for user code.
 */
public interface SilentException {
}
