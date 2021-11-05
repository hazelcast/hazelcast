/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import static java.util.Objects.requireNonNull;

public class HybridLogConfig {

    /**
     * Default page size.
     */
    public static final int DEFAULT_PAGE_SIZE = 4096;
    /**
     * Default mutable region fraction.
     */
    public static final double DEFAULT_MUTABLE_REGION_RATIO = 0.9;

    private int pageSize;
    private int inMemoryPageCount;

    private double mutableRegionRatio;

    public HybridLogConfig() {
        pageSize = DEFAULT_PAGE_SIZE;
        mutableRegionRatio = DEFAULT_MUTABLE_REGION_RATIO;
    }

    public HybridLogConfig(HybridLogConfig hybridLogConfig) {
        requireNonNull(hybridLogConfig);
        pageSize = hybridLogConfig.getPageSize();
        inMemoryPageCount = hybridLogConfig.getInMemoryPageCount();
        mutableRegionRatio = hybridLogConfig.getMutableRegionRatio();
    }

    public int getPageSize() {
        return pageSize;
    }

    public HybridLogConfig setPageSize(int pageSize) {
        this.pageSize = pageSize;
        return this;
    }

    public int getInMemoryPageCount() {
        return inMemoryPageCount;
    }

    public HybridLogConfig setInMemoryPageCount(int inMemoryPageCount) {
        this.inMemoryPageCount = inMemoryPageCount;
        return this;
    }

    public double getMutableRegionRatio() {
        return mutableRegionRatio;
    }

    public HybridLogConfig setMutableRegionRatio(double mutableRegionRatio) {
        this.mutableRegionRatio = mutableRegionRatio;
        return this;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HybridLogConfig)) {
            return false;
        }

        HybridLogConfig that = (HybridLogConfig) o;

        if (pageSize != that.pageSize) {
            return false;
        }
        if (inMemoryPageCount != that.inMemoryPageCount) {
            return false;
        }
        return Double.compare(that.mutableRegionRatio, mutableRegionRatio) == 0;
    }

    @Override
    public final int hashCode() {
        int result;
        long temp;
        result = pageSize;
        result = 31 * result + inMemoryPageCount;
        temp = Double.doubleToLongBits(mutableRegionRatio);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "HybridLogConfig{"
                + "pageSize=" + pageSize
                + ", inMemoryPageCount=" + inMemoryPageCount
                + ", mutableRegionRatio=" + mutableRegionRatio
                + '}';
    }
}
