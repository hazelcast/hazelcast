/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet2.impl;

public abstract class AsyncOperation extends EngineOperation {

    private long callId = -1;

    public AsyncOperation() {
    }

    public AsyncOperation(String engineName) {
        super(engineName);
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public Object getResponse() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void onSetCallId() {
        if (callId == -1) {
            callId = getCallId();
        }
    }

    @Override
    public void beforeRun() throws Exception {
        this.<JetService>getService().registerOperation(getCallerAddress(), getCallId());
    }

    @Override
    public final void run() throws Exception {
        try {
            doRun();
        } catch (Exception e) {
            logError(e);
            doSendResponse(e);
        }
    }

    protected abstract void doRun() throws Exception;

    protected final void doSendResponse(Object value) {
        try {
            sendResponse(value);
        } finally {
            this.<JetService>getService().deregisterOperation(getCallerAddress(), callId);
        }
    }


}
