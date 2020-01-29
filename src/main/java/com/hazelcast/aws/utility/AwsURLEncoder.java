/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.hazelcast.aws.utility;

import com.hazelcast.core.HazelcastException;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

public final class AwsURLEncoder {

    private AwsURLEncoder() {
    }

    public static String urlEncode(String string) {
        String encoded;
        try {
            encoded = URLEncoder.encode(string, "UTF-8").replace("+", "%20");
        } catch (UnsupportedEncodingException e) {
            throw new HazelcastException(e);
        }
        return encoded;
    }
}
