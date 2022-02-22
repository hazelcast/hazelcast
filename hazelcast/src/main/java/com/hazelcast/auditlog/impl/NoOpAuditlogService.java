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

package com.hazelcast.auditlog.impl;

import java.util.Collections;
import java.util.Map;

import com.hazelcast.auditlog.AuditableEvent;
import com.hazelcast.auditlog.AuditlogService;
import com.hazelcast.auditlog.EventBuilder;
import com.hazelcast.auditlog.Level;

public final class NoOpAuditlogService implements AuditlogService {

    public static final NoOpAuditlogService INSTANCE = new NoOpAuditlogService();

    private final Builder builder = new Builder();

    private NoOpAuditlogService() {
    }

    @Override
    public void log(AuditableEvent auditableEvent) {
    }

    @Override
    public void log(String eventTypeId, Level level, String message) {
    }

    @Override
    public void log(String eventTypeId, Level level, String message, Throwable thrown) {
    }

    @Override
    public EventBuilder<?> eventBuilder(String typeId) {
        return builder;
    }

    public static final class NoOpEvent implements AuditableEvent {

        private NoOpEvent() {
        }

        @Override
        public String message() {
            return null;
        }

        @Override
        public String typeId() {
            return "NoOp";
        }

        @Override
        public Map<String, Object> parameters() {
            return Collections.emptyMap();
        }

        @Override
        public Level level() {
            return Level.INFO;
        }

        @Override
        public Throwable cause() {
            return null;
        }

        @Override
        public long getTimestamp() {
            return 0;
        }
    }

    /**
     * Builder to build {@link NoOpEvent}.
     */
    public static final class Builder implements EventBuilder<Builder> {

        private final NoOpEvent event = new NoOpEvent();

        @Override
        public Builder message(String message) {
            return this;
        }

        @Override
        public Builder parameters(Map<String, Object> parameters) {
            return this;
        }

        @Override
        public Builder addParameter(String key, Object value) {
            return this;
        }

        @Override
        public Builder level(Level level) {
            return this;
        }

        @Override
        public Builder cause(Throwable throwable) {
            return this;
        }

        @Override
        public Builder timestamp(long timestamp) {
            return this;
        }

        @Override
        public AuditableEvent build() {
            return event;
        }

        @Override
        public void log() {
        }
    }
}
