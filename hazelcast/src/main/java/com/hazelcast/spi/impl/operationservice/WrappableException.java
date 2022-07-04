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

package com.hazelcast.spi.impl.operationservice;

/**
 * Indicates a runtime exception which can be wrapped in another instance of the same type
 * for the purpose of delivering both a local and an async/remote stack
 * trace, wrapped as the cause of the local exception. This is useful when
 * exceptions contain state other than the plain {@code message} and {@code cause}
 * defined by {@link Throwable}.
 *
 * @see com.hazelcast.spi.impl.AbstractInvocationFuture
 */
public interface WrappableException<T extends RuntimeException> {
    /**
     * Returns a new exception of the same type as {@code this} exception, using
     * {@code this} exception as its cause. This is useful when {@code this} is
     * a remote or async exception, because its stack trace is disconnected from the
     * client code that handles the exception. The returned exception includes
     * all the state of {@code this} exception, while providing the local stack trace
     * and the remote/async stack trace in its {@code cause}.
     *
     * @return  a new {@code WrappableException} with {@code this} as its {@code cause}.
     */
    T wrap();
}
