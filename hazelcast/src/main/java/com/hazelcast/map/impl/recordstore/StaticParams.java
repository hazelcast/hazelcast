/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.recordstore;

/**
 * Represent static parameters of record-store methods.
 */
public final class StaticParams {

    public static final StaticParams PUT_PARAMS = new StaticParams()
            .setPutVanilla(true)
            .setLoad(true)
            .setStore(true)
            .setCheckIfLoaded(true)
            .setCountAsAccess(true);

    public static final StaticParams TRY_PUT_PARAMS = new StaticParams()
            .importFrom(PUT_PARAMS)
            .setTryPut(true);

    public static final StaticParams PUT_BACKUP_PARAMS = new StaticParams()
            .setPutVanilla(true)
            .setCountAsAccess(true)
            .setBackup(true);

    public static final StaticParams PUT_IF_ABSENT_PARAMS = new StaticParams()
            .importFrom(PUT_PARAMS)
            .setPutVanilla(false)
            .setPutIfAbsent(true);

    public static final StaticParams PUT_FROM_LOAD_PARAMS = new StaticParams()
            .setPutVanilla(true)
            .setPutFromLoad(true);

    public static final StaticParams PUT_FROM_LOAD_BACKUP_PARAMS = new StaticParams()
            .importFrom(PUT_FROM_LOAD_PARAMS)
            .setBackup(true);

    public static final StaticParams SET_PARAMS = new StaticParams()
            .importFrom(PUT_PARAMS)
            .setLoad(false);

    public static final StaticParams TXN_SET_PARAMS = new StaticParams()
            .importFrom(SET_PARAMS)
            .setTryPut(true);

    public static final StaticParams SET_WITH_NO_ACCESS_PARAMS = new StaticParams()
            .importFrom(SET_PARAMS)
            .setCountAsAccess(false);

    public static final StaticParams SET_TTL_PARAMS = new StaticParams()
            .setPutIfExists(true)
            .setSetTtl(true)
            .setLoad(true)
            .setStore(true)
            .setCheckIfLoaded(true)
            .setCountAsAccess(true);

    public static final StaticParams SET_TTL_BACKUP_PARAMS = new StaticParams()
            .importFrom(SET_TTL_PARAMS)
            .setBackup(true);

    public static final StaticParams PUT_TRANSIENT_PARAMS = new StaticParams()
            .setPutVanilla(true)
            .setTransient(true)
            .setCheckIfLoaded(true)
            .setCountAsAccess(true);

    public static final StaticParams REPLACE_PARAMS = new StaticParams()
            .setPutIfExists(true)
            .setLoad(true)
            .setStore(true)
            .setCheckIfLoaded(true)
            .setCountAsAccess(true);

    public static final StaticParams REPLACE_IF_SAME_PARAMS = new StaticParams()
            .importFrom(REPLACE_PARAMS)
            .setPutIfEqual(true);

    private boolean backup;
    private boolean load;
    private boolean store;
    // putVanilla: is for regular puts like IMap#set.
    private boolean putVanilla;
    private boolean putIfAbsent;
    private boolean putIfExists;
    private boolean putIfEqual;
    private boolean putFromLoad;
    // isTransient: is for IMap#putTransient.
    private boolean isTransient;
    private boolean setTtl;
    private boolean checkIfLoaded;
    private boolean countAsAccess;
    // tryPut: is for IMap#tryPut.
    private boolean tryPut;

    private StaticParams() {
    }

    private StaticParams importFrom(StaticParams importingFrom) {
        return setBackup(importingFrom.isBackup())
                .setLoad(importingFrom.isLoad())
                .setStore(importingFrom.isStore())
                .setPutVanilla(importingFrom.isPutVanilla())
                .setPutIfAbsent(importingFrom.isPutIfAbsent())
                .setPutIfExists(importingFrom.isPutIfExists())
                .setPutIfEqual(importingFrom.isPutIfEqual())
                .setPutFromLoad(importingFrom.isPutFromLoad())
                .setTransient(importingFrom.isTransient())
                .setSetTtl(importingFrom.isSetTtl())
                .setCheckIfLoaded(importingFrom.isCheckIfLoaded())
                .setCountAsAccess(importingFrom.isCountAsAccess())
                .setTryPut(importingFrom.isTryPut());
    }

    public StaticParams setBackup(boolean backup) {
        this.backup = backup;
        return this;
    }

    public StaticParams setLoad(boolean load) {
        this.load = load;
        return this;
    }

    public StaticParams setStore(boolean store) {
        this.store = store;
        return this;
    }

    public StaticParams setPutVanilla(boolean putVanilla) {
        this.putVanilla = putVanilla;
        return this;
    }

    public StaticParams setPutIfAbsent(boolean putIfAbsent) {
        this.putIfAbsent = putIfAbsent;
        return this;
    }

    public StaticParams setPutIfExists(boolean putIfExists) {
        this.putIfExists = putIfExists;
        return this;
    }

    public StaticParams setPutIfEqual(boolean putIfEqual) {
        this.putIfEqual = putIfEqual;
        return this;
    }

    public StaticParams setPutFromLoad(boolean putFromLoad) {
        this.putFromLoad = putFromLoad;
        return this;
    }

    public StaticParams setTransient(boolean isTransient) {
        this.isTransient = isTransient;
        return this;
    }

    public StaticParams setSetTtl(boolean setTtl) {
        this.setTtl = setTtl;
        return this;
    }

    public StaticParams setCheckIfLoaded(boolean checkIfLoaded) {
        this.checkIfLoaded = checkIfLoaded;
        return this;
    }

    public StaticParams setCountAsAccess(boolean countAsAccess) {
        this.countAsAccess = countAsAccess;
        return this;
    }

    private StaticParams setTryPut(boolean tryPut) {
        this.tryPut = tryPut;
        return this;
    }

    public boolean isBackup() {
        return backup;
    }

    public boolean isLoad() {
        return load;
    }

    public boolean isStore() {
        return store;
    }

    public boolean isPutIfAbsent() {
        return putIfAbsent;
    }

    public boolean isPutVanilla() {
        return putVanilla;
    }

    public boolean isPutIfExists() {
        return putIfExists;
    }

    public boolean isPutIfEqual() {
        return putIfEqual;
    }

    public boolean isPutFromLoad() {
        return putFromLoad;
    }

    public boolean isTransient() {
        return isTransient;
    }

    public boolean isSetTtl() {
        return setTtl;
    }

    public boolean isCheckIfLoaded() {
        return checkIfLoaded;
    }

    public boolean isCountAsAccess() {
        return countAsAccess;
    }

    public boolean isTryPut() {
        return tryPut;
    }
}
