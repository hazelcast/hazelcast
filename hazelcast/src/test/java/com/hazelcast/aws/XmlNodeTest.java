/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import static org.assertj.core.api.Assertions.assertThat;

public class XmlNodeTest {

    @Test
    public void parse() {
        // given
        //language=XML
        String xml = """
                <?xml version="1.0" encoding="UTF-8"?>
                <root xmlns="http://ec2.amazonaws.com/doc/2016-11-15/">
                    <parent>
                        <item>
                            <key>value</key>
                        </item>
                        <item>
                            <key>second-value</key>
                        </item>
                    </parent>
                </root>""";

        // when
        List<String> itemValues = XmlNode.create(xml)
                                         .getSubNodes("parent").stream()
                                         .flatMap(e -> e.getSubNodes("item").stream())
                                         .map(item -> item.getValue("key"))
                                         .toList();

        // then
        assertThat(itemValues).containsExactlyInAnyOrder("value", "second-value");
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
