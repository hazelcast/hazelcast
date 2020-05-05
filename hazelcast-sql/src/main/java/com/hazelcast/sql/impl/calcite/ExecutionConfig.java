/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite;

/**
 * Optimizer configuration.
 */
public final class ExecutionConfig {
    /** Whether optimizer statistics should be collected. */
    private final boolean statisticsEnabled;

    private ExecutionConfig(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
    }

    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private boolean statisticsEnabled;

        private Builder() {
            // No-op.
        }

        public Builder setStatisticsEnabled(boolean statisticsEnabled) {
            this.statisticsEnabled = statisticsEnabled;

            return this;
        }

        public ExecutionConfig build() {
            return new ExecutionConfig(statisticsEnabled);
        }
    }
}
