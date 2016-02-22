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

package com.hazelcast.jet.impl.application.localization.classloader;

import java.io.InputStream;

public class ResourceStream {
    private final String baseUrl;
    private final InputStream inputStream;

    public ResourceStream(InputStream inputStream) {
        this.inputStream = inputStream;
        this.baseUrl = null;
    }

    public ResourceStream(InputStream inputStream, String baseUrl) {
        this.inputStream = inputStream;
        this.baseUrl = baseUrl;
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public InputStream getInputStream() {
        return inputStream;
    }
}
