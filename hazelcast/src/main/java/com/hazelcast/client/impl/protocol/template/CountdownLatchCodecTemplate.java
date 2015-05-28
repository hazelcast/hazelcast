/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.template;

import com.hazelcast.annotation.GenerateCodec;
import com.hazelcast.annotation.Request;
import com.hazelcast.client.impl.protocol.ResponseMessageConst;

@GenerateCodec(id = TemplateConstants.COUNTDOWN_LATCH_TEMPLATE_ID,
        name = "CountDownLatch", ns = "Hazelcast.Client.Protocol.CountDownLatch")
public interface CountdownLatchCodecTemplate {

    @Request(id = 1, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void await(String name, long timeout);

    @Request(id = 2, retryable = false, response = ResponseMessageConst.VOID)
    void countDown(String name);

    @Request(id = 3, retryable = true, response = ResponseMessageConst.INTEGER)
    void getCount(String name);

    @Request(id = 4, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void trySetCount(String name, int count);
}
