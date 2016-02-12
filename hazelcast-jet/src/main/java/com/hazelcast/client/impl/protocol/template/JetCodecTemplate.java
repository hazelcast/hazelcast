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

package com.hazelcast.client.impl.protocol.template;

import com.hazelcast.annotation.GenerateCodec;
import com.hazelcast.annotation.Request;
import com.hazelcast.client.impl.protocol.ResponseMessageConst;
import com.hazelcast.jet.spi.container.CounterKey;
import com.hazelcast.nio.serialization.Data;

import java.util.Map;


@GenerateCodec(id = TemplateConstants.JET_TEMPLATE_ID, name = "Jet", ns = "Hazelcast.Client.Protocol.Codec")
public interface JetCodecTemplate {
    @Request(id = 1, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void init(String name, Data config);

    @Request(id = 2, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void submit(String name, Data dag);

    @Request(id = 3, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void execute(String name);

    @Request(id = 4, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void interrupt(String name);

    @Request(id = 5, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void finalizeApplication(String name);

    @Request(id = 6, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void localize(String name, Data chunk);

    @Request(id = 7, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void acceptLocalization(String name);

    @Request(id = 8, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void event(String name, Data event);

    @Request(id = 9, retryable = false, response = ResponseMessageConst.DATA)
    Map<CounterKey, Object> getAccumulators(String name);
}
