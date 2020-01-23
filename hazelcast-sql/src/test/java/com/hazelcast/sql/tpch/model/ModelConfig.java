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

package com.hazelcast.sql.tpch.model;

/**
 * Configuration of the model.
 */
public class ModelConfig {
    /** Directory with generated files. */
    private final String dir;
    private final int downscale;

    public ModelConfig(String dir, int downscale) {
        this.dir = dir;
        this.downscale = downscale;
    }

    public String getDir() {
        return dir;
    }

    public int getDownscale() {
        return downscale;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String dir;
        private int downscale;

        private Builder() {
            // No-op.
        }

        public Builder setDirectory(String dir) {
            this.dir = dir;

            return this;
        }

        public Builder setDownscale(int downscale) {
            this.downscale = downscale;

            return this;
        }

        public ModelConfig build() {
            return new ModelConfig(dir, downscale);
        }
    }
}
