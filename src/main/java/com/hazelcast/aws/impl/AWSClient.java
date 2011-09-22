/*
 * Copyright (c) 2008-2011, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.aws.impl;

import java.util.List;

public class AWSClient {
    private String accessKey;
    private String secretKey;
    private String endpoint = Constants.HOST_HEADER;

    public AWSClient(String accessKey, String secretKey) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
    }

    public List<String> getPrivateDnsNames(String groupName) throws Exception {
        List<String> list = new DescribeInstances(accessKey, secretKey, groupName).execute(endpoint);
        return list;
    }

    public void setEndpoint(String s) {
        this.endpoint = s;
    }
}
