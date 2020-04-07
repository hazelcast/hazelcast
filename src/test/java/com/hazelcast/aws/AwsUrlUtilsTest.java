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

package com.hazelcast.aws;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;


public class AwsUrlUtilsTest {

    @Test
    public void canonicalQueryString() {
        // given
        Map<String, String> attributes = new HashMap<>();
        attributes.put("second-attribute", "second-attribute+value");
        attributes.put("attribute", "attribute+value");

        // when
        String result = AwsUrlUtils.canonicalQueryString(attributes);

        assertEquals("attribute=attribute%2Bvalue&second-attribute=second-attribute%2Bvalue", result);
    }

}
