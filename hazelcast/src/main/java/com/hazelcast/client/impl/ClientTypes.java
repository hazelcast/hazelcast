/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl;

/**
 * Constants class that contains the different types of clients.
 */
public final class ClientTypes {

    /**
     * Length of type information in bytes
     */
    public static final int TYPE_LENGTH = 3;

    /**
     * JVM clients protocol ID
     */
    public static final String JAVA = "JVM";

    /**
     * Management Center Java client protocol ID
     */
    public static final String MC_JAVA = "MCJ";

    /**
     * CSHARP client protocol ID
     */
    public static final String CSHARP = "CSP";

    /**
     * CPP client protocol ID
     */
    public static final String CPP = "CPP";

    /**
     * PYTHON client protocol ID
     */
    public static final String PYTHON = "PYH";

    /**
     * RUBY client protocol ID
     */
    public static final String RUBY = "RBY";

    /**
     * Node.JS client protocol ID
     */
    public static final String NODEJS = "NJS";

    /**
     * Go client protocol ID
     */
    public static final String GO = "GOO";

    private ClientTypes() {
    }
}
