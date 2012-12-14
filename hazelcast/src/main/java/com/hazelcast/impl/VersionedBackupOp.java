/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.impl;

public class VersionedBackupOp implements Runnable, Comparable {
    final Request request;
    final CMap cmap;

    VersionedBackupOp(CMap cmap, Request request) {
        this.request = request;
        this.cmap = cmap;
    }

    public void run() {
        cmap.doBackup(request);
    }

    long getVersion() {
        return request.version;
    }

    public int compareTo(Object o) {
        long v = ((VersionedBackupOp) o).getVersion();
        if (request.version > v) return 1;
        else if (request.version < v) return -1;
        else return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VersionedBackupOp that = (VersionedBackupOp) o;
        return request.version == that.request.version;
    }

    @Override
    public int hashCode() {
        return (int) (request.version ^ (request.version >>> 32));
    }
}
