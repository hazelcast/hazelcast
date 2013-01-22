///*
// * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package com.hazelcast.spi.impl;
//
//import com.hazelcast.nio.ObjectDataInput;
//import com.hazelcast.nio.ObjectDataOutput;
//import com.hazelcast.nio.serialization.Data;
//import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
//import com.hazelcast.spi.AbstractOperation;
//import com.hazelcast.spi.NodeEngine;
//import com.hazelcast.spi.Operation;
//import com.hazelcast.spi.OperationAccessor;
//
//import java.io.IOException;
//
///**
//* @mdogan 10/1/12
//*/
//
//class OperationWrapper extends AbstractOperation implements IdentifiedDataSerializable {
//
//    private Data operationData;
//
//    private transient Operation operation;
//
//    public OperationWrapper() {
//    }
//
//    public OperationWrapper(final Data operation) {
//        this.operationData = operation;
//    }
//
//    @Override
//    public void beforeRun() throws Exception {
//        final NodeEngine nodeEngine = getNodeEngine();
//        operation = (Operation) nodeEngine.toObject(operationData);
//        operation.setNodeEngine(nodeEngine)
//                .setCaller(getCaller())
//                .setConnection(getConnection())
//                .setServiceName(getServiceName())
//                .setPartitionId(getPartitionId())
//                .setReplicaIndex(getReplicaIndex())
//                .setResponseHandler(getResponseHandler());
//        OperationAccessor.setCallId(operation, getCallId());
//    }
//
//    public void run() throws Exception {
//        if (operation != null) {
//            final NodeEngine nodeEngine = getNodeEngine();
//            nodeEngine.getOperationService().runOperation(operation);
//        }
//    }
//
//    @Override
//    public boolean returnsResponse() {
//        return false;
//    }
//
//    @Override
//    public boolean validatesTarget() {
//        return false;
//    }
//
//    @Override
//    protected void writeInternal(final ObjectDataOutput out) throws IOException {
//        operationData.writeData(out);
//    }
//
//    @Override
//    protected void readInternal(final ObjectDataInput in) throws IOException {
//        operationData = new Data();
//        operationData.readData(in);
//    }
//
//    public int getId() {
//        return DataSerializerSpiHook.OPERATION_WRAPPER;
//    }
//}
