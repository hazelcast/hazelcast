/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import javax.net.ssl.SSLContext;
import java.util.Properties;

/**
 * Factory class for creating {@link javax.net.ssl.SSLContext}
 */
public interface SSLContextFactory {

    /**
     * Initializes this class with config from {@link com.hazelcast.config.SSLConfig}
     *
     * @param properties properties form config
     * @throws Exception
     */
    void init(Properties properties) throws Exception;

    /**
     * @return SSLContext
     */
    SSLContext getSSLContext();
}
