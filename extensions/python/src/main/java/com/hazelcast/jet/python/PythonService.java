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
package com.hazelcast.jet.python;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.python.grpc.InputMessage;
import com.hazelcast.jet.python.grpc.InputMessage.Builder;
import com.hazelcast.jet.python.grpc.JetToPythonGrpc;
import com.hazelcast.jet.python.grpc.JetToPythonGrpc.JetToPythonStub;
import com.hazelcast.jet.python.grpc.OutputMessage;
import com.hazelcast.logging.ILogger;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;

import javax.annotation.Nonnull;
import java.io.File;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * The service object used by the "map using Python" pipeline stage. As a
 * user you don't have to deal with this class directly. It is used when
 * you write {@link PythonTransforms#mapUsingPython
 * stage.apply(PythonService.mapUsingPython(pyConfig))}
 */
final class PythonService {

    private static final String JET_TO_PYTHON_PREFIX = "jet_to_python_";
    static final String MAIN_SHELL_SCRIPT = JET_TO_PYTHON_PREFIX + "main.sh";

    private final ILogger logger;
    private final JetToPythonServer server;
    private final ManagedChannel chan;
    private final StreamObserver<InputMessage> sink;
    private final Queue<CompletableFuture<List<String>>> futureQueue = new ConcurrentLinkedQueue<>();

    private final CountDownLatch completionLatch = new CountDownLatch(1);
    private volatile Throwable exceptionInOutputObserver;

    PythonService(Context procCtx, PythonServiceContext serviceContext) {
        logger = procCtx.logger();
        try {
            server = new JetToPythonServer(serviceContext.runtimeBaseDir(), logger);
            int serverPort = server.start();
            chan = NettyChannelBuilder.forAddress("127.0.0.1", serverPort)
                                      .usePlaintext()
                                      .build();
            JetToPythonStub client = JetToPythonGrpc.newStub(chan);
            sink = client.streamingCall(new OutputMessageObserver());
        } catch (Exception e) {
            throw new JetException("PythonService initialization failed", e);
        }
    }

    /**
     * Returns a service factory configured to create a Python mapping stage.
     */
    @Nonnull
    static ServiceFactory<?, PythonService> factory(@Nonnull PythonServiceConfig cfg) {
        cfg.validate();
        ServiceFactory<PythonServiceContext, PythonService> fac = ServiceFactory
                .withCreateContextFn(ctx -> new PythonServiceContext(ctx, cfg))
                .withDestroyContextFn(PythonServiceContext::destroy)
                .withCreateServiceFn((procCtx, serviceCtx) -> new PythonService(procCtx, serviceCtx))
                .withDestroyServiceFn(PythonService::destroy)
                .withMaxPendingCallsPerProcessor(2);
        if (cfg.baseDir() != null) {
            File baseDir = Objects.requireNonNull(cfg.baseDir());
            return fac.withAttachedDirectory(baseDir.toString(), baseDir);
        } else {
            File handlerFile = Objects.requireNonNull(cfg.handlerFile());
            return fac.withAttachedFile(handlerFile.toString(), handlerFile);
        }
    }

    CompletableFuture<List<String>> sendRequest(List<String> inputBatch) {
        checkForServerError();
        Builder requestBuilder = InputMessage.newBuilder();
        for (String item : inputBatch) {
            requestBuilder.addInputValue(item);
        }
        CompletableFuture<List<String>> future = new CompletableFuture<>();
        futureQueue.add(future);
        sink.onNext(requestBuilder.build());
        return future;
    }

    private void checkForServerError() {
        if (completionLatch.getCount() == 0) {
            throw new JetException("PythonService broke down: " + exceptionInOutputObserver, exceptionInOutputObserver);
        }
    }

    private class OutputMessageObserver implements StreamObserver<OutputMessage> {
        @Override
        public void onNext(OutputMessage outputItem) {
            try {
                futureQueue.remove().complete(outputItem.getOutputValueList());
            } catch (Throwable e) {
                exceptionInOutputObserver = e;
                completionLatch.countDown();
            }
        }

        @Override
        public void onError(Throwable e) {
            try {
                exceptionInOutputObserver = e;
                for (CompletableFuture<List<String>> future; (future = futureQueue.poll()) != null;) {
                    future.completeExceptionally(e);
                }
            } finally {
                completionLatch.countDown();
            }
        }

        @Override
        public void onCompleted() {
            for (CompletableFuture<List<String>> future; (future = futureQueue.poll()) != null;) {
                future.completeExceptionally(new JetException("Completion signaled before the future was completed"));
            }
            completionLatch.countDown();
        }
    }

    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED")
    void destroy() {
        try {
            sink.onCompleted();
            // Stopping the Python subprocess is essential, lower the interrupted flag
            //noinspection ResultOfMethodCallIgnored
            Thread.interrupted();
            if (!completionLatch.await(1, SECONDS)) {
                logger.info("gRPC call has not completed on time");
            }
            if (!chan.shutdown().awaitTermination(1, SECONDS)) {
                logger.info("gRPC client has not shut down on time");
            }
            if (!chan.shutdownNow().awaitTermination(1, SECONDS)) {
                logger.info("gRPC client has not shut down on time, even after forceful shutdown");
            }
            server.stop();
        } catch (Exception e) {
            throw new JetException("PythonService.destroy() failed: " + e, e);
        }
    }
}
