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

package com.hazelcast.jet.grpc.impl;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.grpc.GrpcProperties;
import com.hazelcast.jet.grpc.GrpcService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.properties.HazelcastProperties;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

import javax.annotation.Nonnull;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.jet.grpc.impl.GrpcUtil.translateGrpcException;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class BidirectionalStreamingService<T, R> implements GrpcService<T, R> {

    private final StreamObserver<T> sink;
    private final Queue<CompletableFuture<R>> futureQueue = new ConcurrentLinkedQueue<>();
    private final CountDownLatch completionLatch = new CountDownLatch(1);
    private final ILogger logger;
    private final ManagedChannel channel;
    private volatile Throwable exceptionInOutputObserver;

    private final long destroyTimeout;
    private final long shutdownTimeout;

    public BidirectionalStreamingService(
            @Nonnull Context context,
            @Nonnull ManagedChannel channel,
            @Nonnull FunctionEx<? super ManagedChannel, ? extends FunctionEx<StreamObserver<R>, StreamObserver<T>>>
                    callStubFn
    ) {
        logger = context.logger();
        this.channel = channel;
        sink = callStubFn.apply(channel).apply(new OutputMessageObserver());

        Properties properties = context.hazelcastInstance().getConfig().getProperties();
        HazelcastProperties hzProperties = new HazelcastProperties(properties);
        destroyTimeout = hzProperties.getSeconds(GrpcProperties.DESTROY_TIMEOUT);
        shutdownTimeout = hzProperties.getSeconds(GrpcProperties.SHUTDOWN_TIMEOUT);
    }

    @Nonnull @Override
    public CompletableFuture<R> call(@Nonnull T input) {
        checkForServerError();
        CompletableFuture<R> future = new CompletableFuture<>();
        futureQueue.add(future);
        sink.onNext(input);
        return future;
    }

    private void checkForServerError() {
        if (completionLatch.getCount() == 0) {
            throw new JetException("Exception in gRPC service: " + exceptionInOutputObserver, exceptionInOutputObserver);
        }
    }

    public void destroy() throws InterruptedException {
        sink.onCompleted();

        if (!completionLatch.await(destroyTimeout, SECONDS)) {
            logger.info("gRPC call has not completed on time");
        }
        GrpcUtil.shutdownChannel(channel, logger, shutdownTimeout);
    }

    private class OutputMessageObserver implements StreamObserver<R> {
        @Override
        public void onNext(R outputItem) {
            try {
                futureQueue.remove().complete(outputItem);
            } catch (Throwable e) {
                exceptionInOutputObserver = e;
                completionLatch.countDown();
            }
        }

        @Override
        public void onError(Throwable e) {
            try {
                e = translateGrpcException(e);

                exceptionInOutputObserver = e;
                for (CompletableFuture<R> future; (future = futureQueue.poll()) != null; ) {
                    future.completeExceptionally(e);
                }
            } finally {
                completionLatch.countDown();
            }
        }

        @Override
        public void onCompleted() {
            for (CompletableFuture<R> future; (future = futureQueue.poll()) != null; ) {
                future.completeExceptionally(new JetException("Completion signaled before the future was completed"));
            }
            completionLatch.countDown();
        }
    }
}

