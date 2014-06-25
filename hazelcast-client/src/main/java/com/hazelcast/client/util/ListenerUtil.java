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

import com.hazelcast.client.BaseClientRemoveListenerRequest;
import com.hazelcast.client.ClientRequest;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ClientInvocationServiceImpl;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.Future;

public final class ListenerUtil {

    private ListenerUtil() {
    }

    public static String listen(ClientContext context, ClientRequest request, Object key, EventHandler handler) {
        //TODO callback
        final Future future;
        try {
            final ClientInvocationServiceImpl invocationService = getClientInvocationService(context);

            if (key == null) {
                future = invocationService.invokeOnRandomTarget(request, handler);
            } else {
                future = invocationService.invokeOnKeyOwner(request, key, handler);
            }
            String registrationId = context.getSerializationService().toObject(future.get());
            invocationService.registerListener(registrationId, request.getCallId());
            return registrationId;
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public static boolean stopListening(ClientContext context,
                                        BaseClientRemoveListenerRequest request, String registrationId) {
        try {
            ClientInvocationServiceImpl invocationService = getClientInvocationService(context);
            String realRegistrationId = invocationService.deRegisterListener(registrationId);
            if (realRegistrationId == null) {
                return false;
            }
            request.setRegistrationId(realRegistrationId);
            final Future<Boolean> future = invocationService.invokeOnRandomTarget(request);
            return (Boolean) context.getSerializationService().toObject(future.get());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private static ClientInvocationServiceImpl getClientInvocationService(ClientContext context) {
        return (ClientInvocationServiceImpl) context.getInvocationService();
    }
}
