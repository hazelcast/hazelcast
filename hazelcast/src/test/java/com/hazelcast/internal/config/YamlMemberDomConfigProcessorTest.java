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

package com.hazelcast.internal.config;

import com.hazelcast.config.Config;
import com.hazelcast.internal.yaml.YamlLoader;
import com.hazelcast.internal.yaml.YamlMapping;
import com.hazelcast.internal.yaml.YamlNode;
import org.junit.Assert;
import org.junit.Test;
import org.w3c.dom.Node;

import static com.hazelcast.internal.config.yaml.W3cDomUtil.asW3cNode;

public class YamlMemberDomConfigProcessorTest {

    @Test
    public void shouldRespectDashesInNonStrictMode() throws Exception {
        Node w3cRootNode = toNode("hazelcast:\n  cluster-name: test");

        Config config = new Config();
        new YamlMemberDomConfigProcessor(true, config, false).buildConfig(w3cRootNode);

        Assert.assertEquals("test", config.getClusterName());
    }

    @Test
    public void shouldIgnoreDashesInNonStrictMode() throws Exception {
        Node w3cRootNode = toNode("hazelcast:\n  clustername: test");

        Config config = new Config();
        new YamlMemberDomConfigProcessor(true, config, false).buildConfig(w3cRootNode);

        Assert.assertEquals("test", config.getClusterName());
    }

    @Test
    public void shouldRespectDashesInStrictMode() throws Exception {
        Node w3cRootNode = toNode("hazelcast:\n  clustername: test");

        Config config = new Config();
        new YamlMemberDomConfigProcessor(true, config, true).buildConfig(w3cRootNode);

        Assert.assertEquals(new Config().getClusterName(), config.getClusterName());
    }

    private static Node toNode(String yaml) {
        YamlMapping yamlRootNode = ((YamlMapping) YamlLoader.load(yaml));

        YamlNode imdgRoot = yamlRootNode.childAsMapping(ConfigSections.HAZELCAST.getName());
        if (imdgRoot == null) {
            imdgRoot = yamlRootNode;
        }

        return asW3cNode(imdgRoot);
    }
}
