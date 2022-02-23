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

import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Read-only counterpart of {@link IndexConfig} class.
 */
public class IndexConfigReadOnly extends IndexConfig {

    public IndexConfigReadOnly(IndexConfig other) {
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
    public List<String> getAttributes() {
        List<String> attributes = super.getAttributes();
        List<String> res = new ArrayList<>(attributes);

        return Collections.unmodifiableList(res);
    }

    @Override
    public IndexConfig addAttribute(String attribute) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public IndexConfig setAttributes(List<String> attributes) {
        throw new UnsupportedOperationException("This config is read-only");
    }
}
