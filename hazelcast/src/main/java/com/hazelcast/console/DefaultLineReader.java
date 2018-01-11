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

package com.hazelcast.console;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

/**
 * A {@link LineReader} implemetation.
 */
class DefaultLineReader implements LineReader {

    private BufferedReader in;

    DefaultLineReader() throws UnsupportedEncodingException {
        in = new BufferedReader(new InputStreamReader(System.in, "UTF-8"));
    }

    @Override
    public String readLine() throws Exception {
        return in.readLine();
    }
}
