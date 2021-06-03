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

import com.hazelcast.jet.grpc.impl.BidirectionalStreamingService;
import com.hazelcast.spi.properties.HazelcastProperty;
import io.grpc.stub.StreamObserver;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Properties of the grpc module
 *
 * @since Jet 4.2
 */
public final class GrpcProperties {

    /**
     * Time to wait for {@link StreamObserver#onCompleted()} confirmation when destroying a
     * {@link BidirectionalStreamingService}
     * You may want to increase this value when your gRPC calls take longer than 1 s to cleanly shutdown the service.
     * <p>
     * The default value is 1 s
     */
    public static final HazelcastProperty DESTROY_TIMEOUT
            = new HazelcastProperty("jet.grpc.destroy.timeout.seconds", 1, SECONDS);

    /**
     * Time to wait for clean shutdown of a {@link io.grpc.ManagedChannel}
     * <p>
     * The default value is 1 s
     */
    public static final HazelcastProperty SHUTDOWN_TIMEOUT
            = new HazelcastProperty("jet.grpc.shutdown.timeout.seconds", 1, SECONDS);

    private GrpcProperties() {
    }
}
