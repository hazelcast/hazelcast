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

package com.hazelcast.config.yaml;

import com.hazelcast.internal.yaml.YamlLoader;
import com.hazelcast.internal.yaml.YamlNode;
import com.hazelcast.internal.yaml.YamlTest;
import org.junit.Test;
import org.w3c.dom.Node;

import java.io.InputStream;

public class W3cDomTest {

    @Test
    public void test() {
        InputStream inputStream = YamlTest.class.getClassLoader().getResourceAsStream("yaml-test-root-map.yaml");
        YamlNode root = YamlLoader.load(inputStream, "root-map");
        Node domRoot = W3cDomUtil.asW3cNode(root);
        System.out.println(domRoot);
        System.out.println(domRoot.getNodeName());
        Node item0 = domRoot.getChildNodes().item(0);
        System.out.println(item0.getNodeName());
    }
}
