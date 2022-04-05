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

import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;

public class XmlNodeTest {

    @Test
    public void parse() {
        // given
        //language=XML
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<root xmlns=\"http://ec2.amazonaws.com/doc/2016-11-15/\">\n"
            + "    <parent>\n"
            + "        <item>\n"
            + "            <key>value</key>\n"
            + "        </item>\n"
            + "        <item>\n"
            + "            <key>second-value</key>\n"
            + "        </item>\n"
            + "    </parent>\n"
            + "</root>";

        // when
        List<String> itemValues = XmlNode.create(xml)
            .getSubNodes("parent").stream()
            .flatMap(e -> e.getSubNodes("item").stream())
            .map(item -> item.getValue("key"))
            .collect(Collectors.toList());

        // then
        assertThat(itemValues, hasItems("value", "second-value"));
    }

    @Test(expected = RuntimeException.class)
    public void parseError() {
        // given
        String xml = "malformed-xml";

        // when
        XmlNode.create(xml);

        // then
        // throws exception
    }

}
