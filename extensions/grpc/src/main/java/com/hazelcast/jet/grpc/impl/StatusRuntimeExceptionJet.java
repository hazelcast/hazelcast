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

import com.hazelcast.jet.JetException;
import io.grpc.StatusRuntimeException;

/**
 * {@link io.grpc.StatusRuntimeException} breaks the Serializable contract, see
 * <a href="https://github.com/grpc/grpc-java/issues/1913">gRPC Issue #1913</a>.
 * Jet replaces it with a serializable one.
 */
public class StatusRuntimeExceptionJet extends JetException {

    private static final long serialVersionUID = 1L;

    StatusRuntimeExceptionJet(StatusRuntimeException brokenGrpcException) {
        super(brokenGrpcException.getMessage(), brokenGrpcException.getCause());
        setStackTrace(brokenGrpcException.getStackTrace());
    }
}
