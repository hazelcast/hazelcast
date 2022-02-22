/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationservice;

import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.internal.serialization.SerializationService;

import java.util.Set;

/**
 * This is an internal class. So if you are a regular Hazelcast user, you don't
 * want to use this class. You probably want to use
 * {@link com.hazelcast.core.Offloadable}.
 *
 * The Offload is a {@link CallStatus} designed when an offloaded operation
 * needs to offload the processing of the operation to a different system, e.g
 * a different thread and at some point in the future a response will be ready.
 * Offload instance can be created by Operations that require offloading, for
 * more information see the {@link Operation#call()}.
 *
 * If the operation 'offloads' some work, but doesn't send a response ever,
 * {@link CallStatus#VOID} should be used instead.
 *
 * <h1>Response handling</h1>
 * When the {@link Offload#init(NodeEngineImpl, Set)} is called, the original
 * {@link OperationResponseHandler} of the offloaded operation is decorated with
 * one of designed for the offloading (see heartbeats for more info). This means
 * that sending a response for an offloaded operation, can be done using the
 * regular response handling mechanism
 * (see {@link Operation#sendResponse(Object)} or
 * {@link Operation#getOperationResponseHandler()}. But one should not modify
 * the response handler of an offloaded Operation because otherwise one could
 * e.g. run into a memory leak (the offloaded operation will not be removed
 * from the asynchronous operations).
 *
 * There is an important difference with exception handling. With regular
 * operation when an exception is thrown, first the
 * {@link Operation#onExecutionFailure(Throwable)} is called before the
 * {@link Operation#sendResponse(Object)} is called. But with an offloaded
 * operation, typically the {@link Operation#sendResponse(Object)} is called by
 * the implementation of the offloading logic. So if you need the
 * {@link Operation#onExecutionFailure(Throwable)} to be executed, just as the
 * sendResponse call, the onExecuteFailure call is your own concern.
 *
 * <h1>Heartbeats</h1>
 * The Offload functionality automatically registers the offloaded operation for
 * operation heartbeats. The registration is done when the
 * {@link Offload#init(NodeEngineImpl, Set)} is called. And it is unregistered,
 * as soon as a response is send to the offloaded operation. This is done by
 * decorating updating the original response handler of the operation by wrapping
 * it in a decorating response handler that automatically deregisters on completion.
 */
public abstract class Offload extends CallStatus {

    protected OperationServiceImpl operationService;
    protected NodeEngine nodeEngine;
    protected ExecutionService executionService;
    protected SerializationService serializationService;
    private final Operation offloadedOperation;
    private Set<Operation> asyncOperations;

    /**
     * Creates a new Offload instance.
     *
     * @param offloadedOperation the Operation that is offloaded
     */
    public Offload(final Operation offloadedOperation) {
        super(OFFLOAD_ORDINAL);
        this.offloadedOperation = offloadedOperation;
    }

    /**
     * Returns the Operation that created this Offload. Returned operation
     * should not be null.
     *
     * Currently this is used to automatically register the operation for the
     * sake of heartbeats.
     *
     * @return the Operation.
     */
    public final Operation offloadedOperation() {
        return offloadedOperation;
    }

    /**
     * Initializes the Offload.
     *
     * As part of the initialization, the {@link OperationResponseHandler} of
     * the offloaded {@link Operation} is replaced by a decorated version that
     * takes care of automatic deregistration of the operation on completion.
     *
     * This method is called before the {@link #start()} is called by the
     * Operation infrastructure. An implementor of the {@link Offload} doesn't
     * need to deal with this method.
     *
     * @param nodeEngine the {@link NodeEngineImpl}
     */
    public final void init(NodeEngineImpl nodeEngine, Set<Operation> asyncOperations) {
        this.nodeEngine = nodeEngine;
        this.operationService = nodeEngine.getOperationService();
        this.serializationService = nodeEngine.getSerializationService();
        this.asyncOperations = asyncOperations;
        this.executionService = nodeEngine.getExecutionService();

        asyncOperations.add(offloadedOperation);
        offloadedOperation.setOperationResponseHandler(newOperationResponseHandler());
    }

    private OperationResponseHandler newOperationResponseHandler() {
        OperationResponseHandler delegate = offloadedOperation.getOperationResponseHandler();

        // we need to peel of the OperationResponseHandlerImpl
        if (delegate instanceof OffloadedOperationResponseHandler) {
            delegate = ((OffloadedOperationResponseHandler) delegate).delegate;
        }

        return new OffloadedOperationResponseHandler(delegate);
    }

    /**
     * Starts the actual offloading.
     *
     * This method is still called on the same thread that called the
     * {@link Operation#call()} where this Offload instance was returned. So in
     * most cases you want to schedule some work and then return from this method ASAP so
     * that the thread is released.
     *
     * This method is called after the {@link Operation#afterRun()} is called.
     *
     * It is allowed to call {@link Operation#sendResponse(Object)} in the start
     * method if there is nothing to offload.
     *
     * Note: TenantControl propagation is the implementor's responsibility
     * as it will not automatically be propagated to the offloaded thread.
     *
     * @throws Exception if something fails. If this happens, regular Operation
     *                   exception handling is triggered and normally the
     *                   exception is returned to the caller.
     */
    public abstract void start() throws Exception;

    private class OffloadedOperationResponseHandler implements OperationResponseHandler {
        private final OperationResponseHandler delegate;

        OffloadedOperationResponseHandler(OperationResponseHandler delegate) {
            this.delegate = delegate;
        }

        @Override
        public void sendResponse(Operation op, Object response) {
            asyncOperations.remove(offloadedOperation);
            delegate.sendResponse(offloadedOperation, response);
        }
    }
}
