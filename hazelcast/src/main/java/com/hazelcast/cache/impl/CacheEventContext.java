/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl;

import com.hazelcast.nio.serialization.Data;

/**
 * Wrapper class for parameters of {@link ICacheService#publishEvent(CacheEventContext)}
 */
public class CacheEventContext {

    private String cacheName;
    private CacheEventType eventType;
    private Data dataKey;
    private Data dataValue;
    private Data dataOldValue;
    private boolean isOldValueAvailable;
    private long expirationTime;
    private long accessHit;
    private String origin;
    private int orderKey;
    private int completionId;

    public CacheEventContext() { }

    public String getCacheName() {
        return cacheName;
    }

    public void setCacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    public CacheEventType getEventType() {
        return eventType;
    }

    public void setEventType(CacheEventType eventType) {
        this.eventType = eventType;
    }

    public Data getDataKey() {
        return dataKey;
    }

    public void setDataKey(Data dataKey) {
        this.dataKey = dataKey;
    }

    public Data getDataValue() {
        return dataValue;
    }

    public void setDataValue(Data dataValue) {
        this.dataValue = dataValue;
    }

    public Data getDataOldValue() {
        return dataOldValue;
    }

    public void setDataOldValue(Data dataOldValue) {
        this.dataOldValue = dataOldValue;
    }

    public boolean isOldValueAvailable() {
        return isOldValueAvailable;
    }

    public void setIsOldValueAvailable(boolean isOldValueAvailable) {
        this.isOldValueAvailable = isOldValueAvailable;
    }

    public long getExpirationTime() {
        return expirationTime;
    }

    public void setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
    }

    public long getAccessHit() {
        return accessHit;
    }

    public void setAccessHit(long accessHit) {
        this.accessHit = accessHit;
    }

    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }

    public int getOrderKey() {
        return orderKey;
    }

    public void setOrderKey(int orderKey) {
        this.orderKey = orderKey;
    }

    public int getCompletionId() {
        return completionId;
    }

    public void setCompletionId(int completionId) {
        this.completionId = completionId;
    }
}
