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

package com.hazelcast.client.impl.clientside;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.security.RealmConfigCallback;

/**
 * {@link CallbackHandler} implementation which can be used on client side to access system internals.
 */
public class ClientCallbackHandler implements CallbackHandler {

    private final ClientConfig clientConfig;

    public ClientCallbackHandler(ClientConfig clientConfig) {
        this.clientConfig = clientConfig;
    }

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        for (Callback cb : callbacks) {
            handleCallback(cb);
        }
    }

    protected void handleCallback(Callback cb) throws UnsupportedCallbackException {
        if (cb instanceof RealmConfigCallback) {
            RealmConfigCallback realmCb = (RealmConfigCallback) cb;
            RealmConfig realmCfg = null;
            if (clientConfig != null && clientConfig.getSecurityConfig() != null) {
                realmCfg = clientConfig.getSecurityConfig().getRealmConfig(realmCb.getRealmName());
            }
            realmCb.setRealmConfig(realmCfg);
        } else {
            throw new UnsupportedCallbackException(cb);
        }
    }
}
