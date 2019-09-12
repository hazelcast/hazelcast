/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Read-only counterpart of {@link IndexConfig} class.
 *
 * @deprecated this class will be removed in 4.0; it is meant for internal usage only.
 */
public class IndexConfigReadOnly extends IndexConfig {

    IndexConfigReadOnly(IndexConfig other) {
        super(other);
    }

    @Override
    public IndexConfig setName(String name) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public IndexConfig setType(IndexType type) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public List<IndexAttributeConfig> getAttributes() {
        List<IndexAttributeConfig> attributes = super.getAttributes();
        List<IndexAttributeConfig> res = new ArrayList<>(attributes.size());

        for (IndexAttributeConfig attribute : attributes) {
            res.add(attribute.getAsReadOnly());
        }

        return Collections.unmodifiableList(res);
    }

    @Override
    public IndexConfig addAttribute(IndexAttributeConfig attribute) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public IndexConfig addAttribute(String attribute) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    protected void addAttributeInternal(IndexAttributeConfig attribute) {
        super.addAttributeInternal(attribute.getAsReadOnly());
    }

    @Override
    public IndexConfig setAttributes(List<IndexAttributeConfig> attributes) {
        throw new UnsupportedOperationException("This config is read-only");
    }
}
