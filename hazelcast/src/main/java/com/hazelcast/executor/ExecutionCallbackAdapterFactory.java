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

package com.hazelcast.executor;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.Member;
import com.hazelcast.core.MultiExecutionCallback;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.NodeEngine;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

/**
 * @author mdogan 1/22/13
 */
class ExecutionCallbackAdapterFactory {

    private final MultiExecutionCallback multiExecutionCallback;
    private final ConcurrentMap<Member, ValueWrapper> responses;
    private final Collection<Member> members;
    private final ILogger logger;
    private final AtomicBoolean done = new AtomicBoolean(false);

    ExecutionCallbackAdapterFactory(NodeEngine nodeEngine, Collection<Member> members, MultiExecutionCallback multiExecutionCallback) {
        this.multiExecutionCallback = multiExecutionCallback;
        this.responses = new ConcurrentHashMap<Member, ValueWrapper>(members.size());
        this.members = new HashSet<Member>(members);
        this.logger = nodeEngine.getLogger(ExecutionCallbackAdapterFactory.class.getName());
    }

    private  void onResponse(Member member, Object response) {
        if (done.get()) throw new IllegalStateException("This callback is invalid!");
        if (members.contains(member)) {
            ValueWrapper current = null;
            if ((current = responses.put(member, new ValueWrapper(response))) != null) {
                logger.warning("Replacing current callback value[" + current.value
                        + " with value[" + response + "].");
            }
            try {
                multiExecutionCallback.onResponse(member, response);
            } catch (Throwable e) {
                logger.warning(e.getMessage(), e);
            }
            if (members.size() == responses.size() && done.compareAndSet(false, true)) {
                Map<Member, Object> realResponses = new HashMap<Member, Object>(members.size());
                for (Map.Entry<Member, ValueWrapper> entry : responses.entrySet()) {
                    realResponses.put(entry.getKey(), entry.getValue().value);
                }
                multiExecutionCallback.onComplete(realResponses);
            }
        } else {
            throw new IllegalArgumentException(member + " is not known by this callback!");
        }
    }

    <V> ExecutionCallback<V> callbackFor(Member member) {
        return new InnerExecutionCallback<V>(member);
    }

    private class ValueWrapper {
        final Object value;

        private ValueWrapper(Object value) {
            this.value = value;
        }
    }

    private class InnerExecutionCallback<V> implements ExecutionCallback<V> {
        private final Member member;

        private InnerExecutionCallback(Member member) {
            this.member = member;
        }

        public void onResponse(V response) {
            ExecutionCallbackAdapterFactory.this.onResponse(member, response);
        }

        public void onFailure(Throwable t) {
            ExecutionCallbackAdapterFactory.this.onResponse(member, t);
        }
    }

}
