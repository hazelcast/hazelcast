/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.grpc;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.jet.pipeline.GeneralStage;
import com.hazelcast.jet.pipeline.ServiceFactory;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

/**
 * Represents a gRPC service that can be used as part of a {@link
 * GeneralStage#mapUsingServiceAsync(ServiceFactory, BiFunctionEx)
 * mapUsingServiceAsync} call. Use {@link GrpcServices} to create a service.
 *
 * @param <T> type of the request object
 * @param <R> type of the response object
 *
 * @since Jet 4.1
 */
@FunctionalInterface
public interface GrpcService<T, R> {

    /**
     * Calls the requested service and returns a future which will be
     * completed with the result once a response is received.
     */
    @Nonnull
    CompletableFuture<R> call(@Nonnull T input);
}
