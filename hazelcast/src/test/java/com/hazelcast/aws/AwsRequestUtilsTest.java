/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.aws;

import org.junit.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;


public class AwsRequestUtilsTest {

    @Test
    public void currentTimestamp() {
        // given
        Clock clock = Clock.fixed(Instant.ofEpochMilli(1585909518929L), ZoneId.systemDefault());

        // when
        String currentTimestamp = AwsRequestUtils.currentTimestamp(clock);

        // then
        assertEquals("20200403T102518Z", currentTimestamp);
    }

    @Test
    public void canonicalQueryString() {
        // given
        Map<String, String> attributes = new HashMap<>();
        attributes.put("second-attribute", "second-attribute+value");
        attributes.put("attribute", "attribute+value");
        attributes.put("name", "Name*");

        // when
        String result = AwsRequestUtils.canonicalQueryString(attributes);

        assertEquals("attribute=attribute%2Bvalue&name=Name%2A&second-attribute=second-attribute%2Bvalue", result);
    }

    @Test
    public void urlFor() {
        assertEquals("https://some-endpoint", AwsRequestUtils.urlFor("some-endpoint"));
        assertEquals("https://some-endpoint", AwsRequestUtils.urlFor("https://some-endpoint"));
        assertEquals("http://some-endpoint", AwsRequestUtils.urlFor("http://some-endpoint"));
    }

}
