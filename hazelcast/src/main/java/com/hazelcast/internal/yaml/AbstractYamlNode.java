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

import static com.hazelcast.internal.yaml.YamlUtil.constructPath;

public abstract class AbstractYamlNode implements MutableYamlNode {
    private final YamlNode parent;
    private String nodeName;
    private String path;

    AbstractYamlNode(YamlNode parent, String nodeName) {
        this.parent = parent;
        this.nodeName = nodeName;
        this.path = constructPath(parent, nodeName);
    }

    @Override
    public String nodeName() {
        return nodeName != null ? nodeName : UNNAMED_NODE;
    }

    @Override
    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
        this.path = constructPath(parent, nodeName);
    }

    @Override
    public YamlNode parent() {
        return parent;
    }

    @Override
    public String path() {
        return path;
    }
}
