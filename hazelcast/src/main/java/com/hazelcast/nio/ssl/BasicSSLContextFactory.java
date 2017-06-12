/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.ssl;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import java.util.Properties;

public class BasicSSLContextFactory extends SSLEngineFactorySupport implements SSLContextFactory {

    private SSLContext sslContext;

    @Override
    public void init(Properties properties) throws Exception {
        load(properties);
        KeyManager[] keyManagers = kmf == null ? null : kmf.getKeyManagers();
        TrustManager[] trustManagers = tmf == null ? null : tmf.getTrustManagers();

        sslContext = SSLContext.getInstance(protocol);
        sslContext.init(keyManagers, trustManagers, null);
    }

    @Override
    public SSLContext getSSLContext() {
        return sslContext;
    }

}
