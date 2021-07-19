/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.utils;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

public final class RestClient extends AbstractRestClient<RestClient> {

    protected RestClient(String url) {
        super(url);
    }

    public static RestClient create(String url) {
        return new RestClient(url);
    }

    public RestClient withHeaders(Map<String, String> headers) {
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            withHeader(entry.getKey(), entry.getValue());
        }
        return this;
    }

    protected HttpURLConnection buildHttpConnection(URL urlToConnect) throws IOException {
        return (HttpURLConnection) urlToConnect.openConnection();
    }

    @Override
    protected RestClient self() {
        return this;
    }
}
