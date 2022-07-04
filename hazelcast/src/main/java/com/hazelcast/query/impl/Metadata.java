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

package com.hazelcast.query.impl;

/**
 * Keeps generic metadata for a key value pair. The type of kept metadata
 * is object. It is up to the user of this class to determine the type of
 * metadata and act accordingly.
 */
public class Metadata implements JsonMetadata {

    private Object keyMeta;
    private Object valueMeta;

    public Metadata(Object keyMeta, Object valueMeta) {
        this.keyMeta = keyMeta;
        this.valueMeta = valueMeta;
    }

    public void setKeyMetadata(Object metadata) {
        this.keyMeta = metadata;
    }

    public void setValueMetadata(Object metadata) {
        this.valueMeta = metadata;
    }

    @Override
    public Object getKeyMetadata() {
        return this.keyMeta;
    }

    @Override
    public Object getValueMetadata() {
        return this.valueMeta;
    }
}
