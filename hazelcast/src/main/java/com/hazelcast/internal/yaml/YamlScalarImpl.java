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

public class YamlScalarImpl extends AbstractYamlNode implements MutableYamlScalar {
    private Object value;

    public YamlScalarImpl(YamlNode parent, String nodeName, Object value) {
        super(parent, nodeName);
        this.value = value;
    }

    @Override
    public <T> boolean isA(Class<T> type) {
        return value != null && value.getClass().isAssignableFrom(type);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T nodeValue() {
        return (T) value;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T nodeValue(Class<T> type) {
        if (!isA(type)) {
            throw new YamlException("The scalar's type " + value.getClass() + " is not the expected " + type);
        }
        return (T) value;
    }

    @Override
    public void setValue(Object newValue) {
        value = newValue;
    }

    @Override
    public String toString() {
        return "YamlScalarImpl{"
                + "nodeName=" + nodeName()
                + ", value=" + value
                + '}';
    }
}
