/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.json.impl;

import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.JsonParser;
import tools.jackson.jr.ob.JacksonJrExtension;
import tools.jackson.jr.ob.api.ExtensionContext;
import tools.jackson.jr.ob.api.ReaderWriterProvider;
import tools.jackson.jr.ob.api.ValueReader;
import tools.jackson.jr.ob.api.ValueWriter;
import tools.jackson.jr.ob.impl.JSONReader;
import tools.jackson.jr.ob.impl.JSONWriter;

import static com.hazelcast.jet.json.impl.JsonUtilImpl.JSON_DEFAULT_BLOCKLIST_HINT;
import static com.hazelcast.jet.json.impl.JsonUtilImpl.isBlockedByDefaultBlocklist;

public class JacksonJrFilteringExtension extends JacksonJrExtension {

    @Override
    protected void register(ExtensionContext ctxt) {
        if (!Boolean.getBoolean(JsonUtilImpl.JSON_BLOCKLIST_DEFAULTS_DISABLED_PROPERTY)) {
            ctxt.insertProvider(new ReaderWriterProvider() {
                @Override
                public ValueReader findValueReader(JSONReader readContext, Class<?> type) {
                    if (isBlockedByDefaultBlocklist(type)) {
                        return new RejectingReader(type);
                    }
                    return super.findValueReader(readContext, type);
                }

                @Override
                public ValueWriter findValueWriter(JSONWriter writeContext, Class<?> type) {
                    if (isBlockedByDefaultBlocklist(type)) {
                        return new RejectingWriter(type);
                    }
                    return super.findValueWriter(writeContext, type);
                }
            });
        }
    }

    private static class RejectingReader extends ValueReader {
        private final Class<?> type;

        RejectingReader(Class<?> type) {
            super(type);
            this.type = type;
        }

        @Override
        public Object read(JSONReader reader, JsonParser p) throws JacksonException {
            throw new IllegalArgumentException(type + " cannot be deserialized using JSON" + JSON_DEFAULT_BLOCKLIST_HINT);
        }
    }

    private static class RejectingWriter implements ValueWriter {
        private final Class<?> type;

        RejectingWriter(Class<?> type) {
            this.type = type;
        }

        @Override
        public void writeValue(JSONWriter context, JsonGenerator g, Object value) throws JacksonException {
            throw new IllegalArgumentException(type + " cannot be serialized using JSON" + JSON_DEFAULT_BLOCKLIST_HINT);
        }

        @Override
        public Class<?> valueType() {
            return type;
        }
    }
}
