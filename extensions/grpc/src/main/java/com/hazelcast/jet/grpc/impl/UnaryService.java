/*
 * Copyright 2020 Hazelcast Inc.
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

package com.hazelcast.jet.grpc.impl;

import com.hazelcast.function.BiConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.grpc.GrpcService;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

public final class UnaryService<T, R> implements GrpcService<T, R> {

    private final BiConsumerEx<? super T, ? super StreamObserver<R>> callFn;

    public UnaryService(
            @Nonnull ManagedChannel channel,
            @Nonnull FunctionEx<? super ManagedChannel, ? extends BiConsumerEx<T, StreamObserver<R>>> callStubFn
    ) {
        callFn = callStubFn.apply(channel);
    }

    @Override @Nonnull
    public CompletableFuture<R> call(@Nonnull T input) {
        Observer<R> o = new Observer<>();
        callFn.accept(input, o);
        return o.future;
    }

    public void destroy() {
    }

    private static class Observer<R> implements StreamObserver<R> {
        private final CompletableFuture<R> future;

        private volatile R value ;

        Observer() {
            this.future = new CompletableFuture<>();
        }

        @Override
        public void onNext(R value) {
            this.value = value;
        }

        @Override
        public void onError(Throwable t) {
            future.completeExceptionally(GrpcUtil.translateGrpcException(t));
        }

        @Override
        public void onCompleted() {
            future.complete(value);
        }
    }
}

