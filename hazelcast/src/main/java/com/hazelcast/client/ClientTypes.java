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

package com.hazelcast.client;

import com.hazelcast.spi.annotation.PrivateApi;

/**
 * Constants class that contains the different types of clients.
 */
@PrivateApi
public final class ClientTypes {

    /**
     * Length of type information in bytes
     */
    public static final int TYPE_LENGTH = 3;

    /**
     * Jvm clients protocol id
     */
    public static final String JAVA = "JVM";

    /**
     * CSHARP client protocol id
     */
    public static final String CSHARP = "CSP";

    /**
     * CPP client protocol id
     */
    public static final String CPP = "CPP";

    /**
     * PYTHON client protocol id
     */
    public static final String PYTHON = "PYH";

    /**
     * RUBY client protocol id
     */
    public static final String RUBY = "RBY";

    /**
     * Node.JS client protocol id
     */
    public static final String NODEJS = "NJS";

    private ClientTypes() {
    }
}
