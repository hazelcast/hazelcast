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

package com.hazelcast.map.impl.recordstore;

/**
 * Represent static parameters of record-store methods.
 */
final class StaticParams {

    static final StaticParams PUT_PARAMS = new StaticParams()
            .setPutVanilla(true)
            .setLoad(true)
            .setStore(true)
            .setCheckIfLoaded(true)
            .setCountAsAccess(true);

    static final StaticParams PUT_BACKUP_PARAMS = new StaticParams()
            .setPutVanilla(true)
            .setCountAsAccess(true)
            .setBackup(true);

    static final StaticParams PUT_IF_ABSENT_PARAMS = new StaticParams()
            .setPutIfAbsent(true)
            .setLoad(true)
            .setStore(true)
            .setCheckIfLoaded(true)
            .setCountAsAccess(true);

    static final StaticParams PUT_FROM_LOAD_PARAMS = new StaticParams()
            .setPutVanilla(true)
            .setPutFromLoad(true);

    static final StaticParams PUT_FROM_LOAD_BACKUP_PARAMS = new StaticParams()
            .setPutVanilla(true)
            .setPutFromLoad(true)
            .setBackup(true);

    static final StaticParams SET_PARAMS = new StaticParams()
            .setPutVanilla(true)
            .setStore(true)
            .setCheckIfLoaded(true)
            .setCountAsAccess(true);

    static final StaticParams SET_WTH_NO_ACCESS_PARAMS = new StaticParams()
            .setPutVanilla(true)
            .setStore(true)
            .setCheckIfLoaded(true);

    static final StaticParams SET_TTL_PARAMS = new StaticParams()
            .setPutIfExists(true)
            .setSetTtl(true)
            .setLoad(true)
            .setStore(true)
            .setCheckIfLoaded(true)
            .setCountAsAccess(true);

    static final StaticParams SET_TTL_BACKUP_PARAMS = new StaticParams()
            .setPutIfExists(true)
            .setSetTtl(true)
            .setLoad(true)
            .setStore(true)
            .setCheckIfLoaded(true)
            .setCountAsAccess(true)
            .setBackup(true);

    static final StaticParams PUT_TRANSIENT_PARAMS = new StaticParams()
            .setPutVanilla(true)
            .setTransient(true)
            .setCheckIfLoaded(true)
            .setCountAsAccess(true);

    static final StaticParams REPLACE_PARAMS = new StaticParams()
            .setPutIfExists(true)
            .setLoad(true)
            .setStore(true)
            .setCheckIfLoaded(true)
            .setCountAsAccess(true);

    static final StaticParams REPLACE_IF_SAME_PARAMS = new StaticParams()
            .setPutIfExists(true)
            .setPutIfEqual(true)
            .setLoad(true)
            .setStore(true)
            .setCheckIfLoaded(true)
            .setCountAsAccess(true);

    private boolean backup;
    private boolean load;
    private boolean store;
    private boolean putVanilla;
    private boolean putIfAbsent;
    private boolean putIfExists;
    private boolean putIfEqual;
    private boolean putFromLoad;
    private boolean isTransient;
    private boolean setTtl;
    private boolean checkIfLoaded;
    private boolean countAsAccess;

    private StaticParams() {
    }

    public StaticParams setBackup(boolean backup) {
        this.backup = backup;
        return this;
    }

    StaticParams setLoad(boolean load) {
        this.load = load;
        return this;
    }

    StaticParams setStore(boolean store) {
        this.store = store;
        return this;
    }

    StaticParams setPutVanilla(boolean putVanilla) {
        this.putVanilla = putVanilla;
        return this;
    }

    StaticParams setPutIfAbsent(boolean putIfAbsent) {
        this.putIfAbsent = putIfAbsent;
        return this;
    }

    StaticParams setPutIfExists(boolean putIfExists) {
        this.putIfExists = putIfExists;
        return this;
    }

    StaticParams setPutIfEqual(boolean putIfEqual) {
        this.putIfEqual = putIfEqual;
        return this;
    }

    StaticParams setPutFromLoad(boolean putFromLoad) {
        this.putFromLoad = putFromLoad;
        return this;
    }

    StaticParams setTransient(boolean isTransient) {
        this.isTransient = isTransient;
        return this;
    }

    StaticParams setSetTtl(boolean setTtl) {
        this.setTtl = setTtl;
        return this;
    }

    StaticParams setCheckIfLoaded(boolean checkIfLoaded) {
        this.checkIfLoaded = checkIfLoaded;
        return this;
    }

    StaticParams setCountAsAccess(boolean countAsAccess) {
        this.countAsAccess = countAsAccess;
        return this;
    }

    boolean isBackup() {
        return backup;
    }

    boolean isLoad() {
        return load;
    }

    boolean isStore() {
        return store;
    }

    boolean isPutIfAbsent() {
        return putIfAbsent;
    }

    boolean isPutVanilla() {
        return putVanilla;
    }

    boolean isPutIfExists() {
        return putIfExists;
    }

    boolean isPutIfEqual() {
        return putIfEqual;
    }

    boolean isPutFromLoad() {
        return putFromLoad;
    }

    boolean isTransient() {
        return isTransient;
    }

    boolean isSetTtl() {
        return setTtl;
    }

    boolean isCheckIfLoaded() {
        return checkIfLoaded;
    }

    boolean isCountAsAccess() {
        return countAsAccess;
    }
}
