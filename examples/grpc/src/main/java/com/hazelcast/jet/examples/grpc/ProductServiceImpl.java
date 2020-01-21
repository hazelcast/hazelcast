/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.examples.grpc;

import com.hazelcast.jet.examples.grpc.ProductServiceGrpc.ProductServiceImplBase;
import com.hazelcast.jet.examples.grpc.datamodel.Product;
import io.grpc.stub.StreamObserver;

import java.util.Map;

/**
 * Server-side implementation of a gRPC service. See {@link
 * GRPCEnrichment#enrichUsingGRPC()}.
 */
public class ProductServiceImpl extends ProductServiceImplBase {

    private final Map<Integer, Product> products;

    public ProductServiceImpl(Map<Integer, Product> products) {
        this.products = products;
    }

    @Override
    public void productInfo(ProductInfoRequest request, StreamObserver<ProductInfoReply> responseObserver) {
        String productName = products.get(request.getId()).name();
        ProductInfoReply reply = ProductInfoReply.newBuilder().setProductName(productName).build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }
}
