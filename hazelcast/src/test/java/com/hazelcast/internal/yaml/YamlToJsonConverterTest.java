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

package com.hazelcast.internal.yaml;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class YamlToJsonConverterTest {

    @Test
    public void emptyObject() {
        YamlMapping root = (YamlMapping) YamlLoader.load(
                getClass().getResourceAsStream("/com/hazelcast/config/empty-object.yaml"));
        JSONObject actual = (JSONObject) YamlToJsonConverter.convert(root);
        assertEquals(JSONObject.NULL, actual.getJSONObject("hazelcast").getJSONObject("network").get("memcache-protocol"));
    }

    @Test
    public void convertSuccess() {
        String expectedJson = "{\n"
                + "   \"hazelcast\":{\n"
                + "      \"network\":{\n"
                + "         \"port\":{\n"
                + "            \"auto-increment\":true,\n"
                + "            \"port-count\":100,\n"
                + "            \"port\":5701,\n"
                + "            \"outbound-ports\":[\n"
                + "               \"33000-35000\",\n"
                + "               \"37000,37001,37002,37003\",\n"
                + "               \"38000,38500-38600\"\n"
                + "            ]\n"
                + "         },\n"
                + "         \"public-address\":\"dummy\"\n"
                + "      }\n"
                + "   }\n"
                + "}";
        JSONObject expectedJsonObject = new JSONObject(expectedJson);
        YamlMappingImpl parentNode = createYamlMapping();

        Object converted = YamlToJsonConverter.convert(parentNode);

        assertTrue(expectedJsonObject.similar(converted));
    }

    @Test
    public void convertUnknownNode() {
        YamlMappingImpl parentNode = createYamlMapping();
        YamlMappingImpl hazelcast = (YamlMappingImpl) parentNode.childAsMapping("hazelcast");
        YamlNode unknownNode = new YamlNode() {
            @Override
            public YamlNode parent() {
                return hazelcast;
            }

            @Override
            public String nodeName() {
                return "unknown-node";
            }

            @Override
            public String path() {
                return "null";
            }
        };
        hazelcast.addChild("unknown-node", unknownNode);

        Exception exception = assertThrows(IllegalArgumentException.class,
                () -> YamlToJsonConverter.convert(parentNode));
        assertTrue(exception.getMessage().contains("Unknown type "));
    }

    private YamlMappingImpl createYamlMapping() {
        YamlMappingImpl parentNode = new YamlMappingImpl(null, null);
        YamlMappingImpl hazelcastNode = new YamlMappingImpl(parentNode, "hazelcast");
        parentNode.addChild("hazelcast", hazelcastNode);

        YamlMappingImpl networkNode = new YamlMappingImpl(hazelcastNode, "network");
        hazelcastNode.addChild("network", networkNode);
        YamlScalarImpl publicAddressNode = new YamlScalarImpl(networkNode, "public-address", "dummy");
        networkNode.addChild("public-address", publicAddressNode);

        YamlMappingImpl portNode = new YamlMappingImpl(networkNode, "port");
        networkNode.addChild("port", portNode);
        YamlScalarImpl autoIncrementNode = new YamlScalarImpl(portNode, "auto-increment", true);
        portNode.addChild("auto-increment", autoIncrementNode);
        YamlScalarImpl portCountNode = new YamlScalarImpl(portNode, "port-count", 100);
        portNode.addChild("port-count", portCountNode);
        YamlScalarImpl portScalarNode = new YamlScalarImpl(portNode, "port", 5701);
        portNode.addChild("port", portScalarNode);

        YamlSequenceImpl outboundPortsSequence = new YamlSequenceImpl(networkNode, "outbound-ports");
        portNode.addChild("outbound-ports", outboundPortsSequence);
        YamlScalarImpl outboundPortNode1 = new YamlScalarImpl(outboundPortsSequence, null, "33000-35000");
        YamlScalarImpl outboundPortNode2 = new YamlScalarImpl(outboundPortsSequence, null, "37000,37001,37002,37003");
        YamlScalarImpl outboundPortNode3 = new YamlScalarImpl(outboundPortsSequence, null, "38000,38500-38600");
        outboundPortsSequence.addChild(outboundPortNode1);
        outboundPortsSequence.addChild(outboundPortNode2);
        outboundPortsSequence.addChild(outboundPortNode3);
        return parentNode;
    }

}
