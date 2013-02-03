/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client;

import com.hazelcast.core.Member;
import com.hazelcast.util.ResponseQueueFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class Call {

    private final Long id;

    private final Packet request;

    private volatile Object response;

    private final BlockingQueue<Object> responseQueue = ResponseQueueFactory.newResponseQueue();

    volatile long sent = 0;

    volatile long written = 0;

    volatile long received = 0;

    volatile long replied = 0;

    private boolean fireNforget;

    public Call(Long id, Packet request) {
        this.id = id;
        this.request = request;
        if (request != null) {
            this.request.setCallId(id);
        }
    }

    public Packet getRequest() {
        return request;
    }

    public Long getId() {
        return id;
    }

    public Object getResponse() {
        try {
            Object res = (response != null) ? response : responseQueue.take();
            return handleResponse(res);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private Object handleResponse(Object response) {
        if (response == null) {
            return null;
        } else if (response instanceof RuntimeException) {
            throw (RuntimeException) response;
        } else {
            return response;
        }
    }

    public Object getResponse(long timeout, TimeUnit unit) {
        try {
            Object res = (response != null) ? response : responseQueue.poll(timeout, unit);
            return handleResponse(res);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void onDisconnect(Member member) {
    }

    public boolean hasResponse() {
        return this.response != null || responseQueue.size() > 0;
    }

    public void setResponse(Object response) {
        this.response = response;
        this.responseQueue.offer(response);
    }

    public boolean isFireNforget() {
        return fireNforget;
    }

    public void setFireNforget(boolean fireNforget) {
        this.fireNforget = fireNforget;
    }

    @Override
    public String toString() {
        return "Call " + "[" + id + "] operation=" +
                (request != null ? request.getOperation() : null);
    }
}
