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

import com.hazelcast.logging.ILogger;
import io.grpc.ManagedChannel;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;

import static java.util.concurrent.TimeUnit.SECONDS;

public final class GrpcUtil {

    private GrpcUtil() {
    }

    /**
     * {@link io.grpc.StatusException} and {@link io.grpc.StatusRuntimeException}
     * break the Serializable contract, see
     * <a href="https://github.com/grpc/grpc-java/issues/1913">gRPC Issue #1913</a>.
     * This method replaces them with serializable ones.
     *
     * @param exception the exception to examine and possibly replace
     * @return the same exception or a replacement if needed
     */
    public static Throwable translateGrpcException(Throwable exception) {
        if (exception instanceof StatusException) {
            return new StatusExceptionJet((StatusException) exception);
        } else if (exception instanceof StatusRuntimeException) {
            return new StatusRuntimeExceptionJet((StatusRuntimeException) exception);
        }
        return exception;
    }

    /**
     * Shutdowns {@link ManagedChannel}
     * <p>
     * Tries orderly shutdown first {@link ManagedChannel#shutdown()}, then forceful shutdown
     * {@link ManagedChannel#shutdownNow()}.
     */
    public static void shutdownChannel(ManagedChannel channel, ILogger logger) throws InterruptedException {
        if (!channel.shutdown().awaitTermination(1, SECONDS)) {
            logger.info("gRPC client has not shut down on time");

            if (!channel.shutdownNow().awaitTermination(1, SECONDS)) {
                logger.info("gRPC client has not shut down on time, even after forceful shutdown");
            }
        }
    }
}
