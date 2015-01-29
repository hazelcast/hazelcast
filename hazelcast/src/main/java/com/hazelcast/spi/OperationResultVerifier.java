/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi;

/**
 * This interface can be implemented and set to an {@link com.hazelcast.spi.InternalCompletableFuture}
 * instance to verify a returned result by additional checks like type checking.
 *
 * @param <V> value type to be checked
 */
public interface OperationResultVerifier<V> {

    /**
     * Performs an additional verification step on a given value and may change it, wrap it or return
     * an exception to show the value is wrong.<p>
     * This method is called on an {@link com.hazelcast.spi.InternalCompletableFuture} response resolve
     * call and is executed on the resolving thread.<p>
     * To configure an {@link com.hazelcast.spi.OperationResultVerifier} call
     * {@link com.hazelcast.spi.InternalCompletableFuture#setOperationResultVerifier(OperationResultVerifier)}
     * and pass in the verifier to execute.
     *
     * @param value value to be checked
     *
     * @return the given value if verification is fine or another wrapped value or an exception,
     *         the exception is not wrapped into an {@link java.util.concurrent.ExecutionException}
     *         if used together with a {@link InternalCompletableFuture#get()} call.
     */
    Object verify(V value);

}
