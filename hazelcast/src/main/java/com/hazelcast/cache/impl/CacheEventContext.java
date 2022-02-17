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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.CacheEventType;
import com.hazelcast.internal.serialization.Data;

import java.util.UUID;

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
    private long creationTime;
    private long expirationTime;
    private long lastAccessTime;
    private long accessHit;
    private UUID origin;
    private int orderKey;
    private int completionId;
    private Data expiryPolicy;

    public CacheEventContext() { }

    public String getCacheName() {
        return cacheName;
    }

    public CacheEventContext setCacheName(String cacheName) {
        this.cacheName = cacheName;
        return this;
    }

    public CacheEventType getEventType() {
        return eventType;
    }

    public CacheEventContext setEventType(CacheEventType eventType) {
        this.eventType = eventType;
        return this;
    }

    public Data getDataKey() {
        return dataKey;
    }

    public CacheEventContext setDataKey(Data dataKey) {
        this.dataKey = dataKey;
        return this;
    }

    public Data getDataValue() {
        return dataValue;
    }

    public CacheEventContext setDataValue(Data dataValue) {
        this.dataValue = dataValue;
        return this;
    }

    public Data getDataOldValue() {
        return dataOldValue;
    }

    public CacheEventContext setDataOldValue(Data dataOldValue) {
        this.dataOldValue = dataOldValue;
        return this;
    }

    public boolean isOldValueAvailable() {
        return isOldValueAvailable;
    }

    public CacheEventContext setIsOldValueAvailable(boolean isOldValueAvailable) {
        this.isOldValueAvailable = isOldValueAvailable;
        return this;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public CacheEventContext setCreationTime(long creationTime) {
        this.creationTime = creationTime;
        return this;
    }

    public long getExpirationTime() {
        return expirationTime;
    }

    public CacheEventContext setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
        return this;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    public long getAccessHit() {
        return accessHit;
    }

    public CacheEventContext setAccessHit(long accessHit) {
        this.accessHit = accessHit;
        return this;
    }

    public Data getExpiryPolicy() {
        return expiryPolicy;
    }

    public CacheEventContext setExpiryPolicy(Data expiryPolicy) {
        this.expiryPolicy = expiryPolicy;
        return this;
    }

    public UUID getOrigin() {
        return origin;
    }

    public CacheEventContext setOrigin(UUID origin) {
        this.origin = origin;
        return this;
    }

    public int getOrderKey() {
        return orderKey;
    }

    public CacheEventContext setOrderKey(int orderKey) {
        this.orderKey = orderKey;
        return this;
    }

    public int getCompletionId() {
        return completionId;
    }

    public CacheEventContext setCompletionId(int completionId) {
        this.completionId = completionId;
        return this;
    }

}
