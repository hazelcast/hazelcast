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

/**
 * Contributes {@link com.hazelcast.jet.grpc.GrpcServices
 * gRPC service factories} that can be to apply transformations to
 * a pipeline which for each input item calls to a gRPC service.
 *
 * @since Jet 4.1
 */
@EvolvingApi
package com.hazelcast.jet.grpc;

import com.hazelcast.jet.annotation.EvolvingApi;
