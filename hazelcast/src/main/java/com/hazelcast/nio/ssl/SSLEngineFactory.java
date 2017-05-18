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

import javax.net.ssl.SSLEngine;
import java.util.Properties;

/**
 * The {@link SSLEngineFactory} is responsible for creating an SSLEngine instance.
 */
public interface SSLEngineFactory {

    /**
     * Initializes this class with config from {@link com.hazelcast.config.SSLConfig}
     *
     * @param properties properties form config
     * @throws Exception
     */
    void init(Properties properties) throws Exception;


    /**
     * Creates a SSLEngine.
     *
     * @param clientMode if the SSLEngine should be in client mode, or server-mode. See {@link SSLEngine#getUseClientMode()}.
     * @return the created SSLEngine.
     */
    SSLEngine create(boolean clientMode);
}
