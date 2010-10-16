/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.client;

import com.hazelcast.core.Member;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Call {

    private final Long id;

    private final Packet request;

    private final BlockingQueue<Object> responseQueue = new LinkedBlockingQueue<Object>();

    public Call(Long id, Packet request) {
        this.id = id;
        this.request = request;
        this.request.setCallId(id);
    }

    public Packet getRequest() {
        return request;
    }

    public Long getId() {
        return id;
    }

    public Object getResponse() {
        try {
            return handleResponse(responseQueue.take());
        } catch (InterruptedException ignored) {
            return null;
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
            return handleResponse(responseQueue.poll(timeout, unit));
        } catch (InterruptedException ignored) {
            return null;
        }
    }
    
    public void onDisconnect(Member member) {
    }

    public void setResponse(Object response) {
        responseQueue.offer(response);
    }

    @Override
    public String toString() {
        return "Call " + "[" + id + "] operation=" + request.getOperation();
    }
}
