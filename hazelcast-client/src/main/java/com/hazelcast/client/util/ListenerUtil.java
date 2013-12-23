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

package com.hazelcast.client.util;

import com.hazelcast.client.ClientRequest;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.Future;

/**
 * @author ali 23/12/13
 */
public final class ListenerUtil {

    public static String listen(ClientContext context, ClientRequest request, Object key, EventHandler handler) {
        //TODO callback
        final Future<String> future;
        try {
            if (key == null) {
                future = context.getInvocationService().invokeOnRandomTarget(request, handler);
            } else {
                future = context.getInvocationService().invokeOnKeyOwner(request, key, handler);
            }
            String registrationId = future.get();
            context.getClusterService().registerListener(registrationId, request.getCallId());
            return registrationId;
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public static String listen(HazelcastClient client, ClientRequest request, Object key, EventHandler handler) {
        //TODO callback
        final Future<String> future;
        try {
            if (key == null) {
                future = client.getInvocationService().invokeOnRandomTarget(request, handler);
            } else {
                future = client.getInvocationService().invokeOnKeyOwner(request, key, handler);
            }
            String registrationId = future.get();
            client.getClientClusterService().registerListener(registrationId, request.getCallId());
            return registrationId;
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public static boolean stopListening(ClientContext context, ClientRequest request, String registrationId){
        final Future<Boolean> future;
        try {
            future = context.getInvocationService().invokeOnRandomTarget(request);
            Boolean result = future.get();
            context.getClusterService().deRegisterListener(registrationId);
            return result;
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }



}
