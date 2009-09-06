package com.hazelcast.impl;

public class VersionedBackupOp implements Runnable, Comparable {
        final Request request;
        final ConcurrentMapManager.CMap cmap;
        VersionedBackupOp(ConcurrentMapManager.CMap cmap, Request request) {
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