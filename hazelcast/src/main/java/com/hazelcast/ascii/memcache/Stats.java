/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.ascii.memcache;

public class Stats {


    private int waitingRequests;
    private int threads;
    //seconds
    private int uptime;
    private long cmdGet;
    private long cmdSet;
    private long cmdTouch;
    private long getHits;
    private long getMisses;
    private long deleteHits;
    private long deleteMisses;
    private long incrHits;
    private long incrMisses;
    private long decrHits;
    private long decrMisses;
    private long bytes;
    private int currConnections;
    private int totalConnections;

    public Stats() {
    }

    public void setDeleteHits(long deleteHits) {
        this.deleteHits = deleteHits;
    }

    public void setWaitingRequests(int waitingRequests) {
        this.waitingRequests = waitingRequests;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

    public void setUptime(int uptime) {
        this.uptime = uptime;
    }

    public void setCmdGet(long cmdGet) {
        this.cmdGet = cmdGet;
    }

    public void setCmdSet(long cmdSet) {
        this.cmdSet = cmdSet;
    }

    public void setCmdTouch(long cmdTouch) {
        this.cmdTouch = cmdTouch;
    }

    public void setGetHits(long getHits) {
        this.getHits = getHits;
    }

    public void setGetMisses(long getMisses) {
        this.getMisses = getMisses;
    }

    public void setDeleteMisses(long deleteMisses) {
        this.deleteMisses = deleteMisses;
    }

    public void setIncrHits(long incrHits) {
        this.incrHits = incrHits;
    }

    public void setIncrMisses(long incrMisses) {
        this.incrMisses = incrMisses;
    }

    public void setDecrHits(long decrHits) {
        this.decrHits = decrHits;
    }

    public void setDecrMisses(long decrMisses) {
        this.decrMisses = decrMisses;
    }

    public void setBytes(long bytes) {
        this.bytes = bytes;
    }

    public void setCurrConnections(int currConnections) {
        this.currConnections = currConnections;
    }

    public void setTotalConnections(int totalConnections) {
        this.totalConnections = totalConnections;
    }

    public int getWaitingRequests() {
        return waitingRequests;
    }

    public int getThreads() {
        return threads;
    }

    public int getUptime() {
        return uptime;
    }

    public long getCmdGet() {
        return cmdGet;
    }

    public long getCmdSet() {
        return cmdSet;
    }

    public long getCmdTouch() {
        return cmdTouch;
    }

    public long getGetHits() {
        return getHits;
    }

    public long getGetMisses() {
        return getMisses;
    }

    public long getDeleteHits() {
        return deleteHits;
    }

    public long getDeleteMisses() {
        return deleteMisses;
    }

    public long getIncrHits() {
        return incrHits;
    }

    public long getIncrMisses() {
        return incrMisses;
    }

    public long getDecrHits() {
        return decrHits;
    }

    public long getDecrMisses() {
        return decrMisses;
    }

    public long getBytes() {
        return bytes;
    }

    public int getCurrConnections() {
        return currConnections;
    }

    public int getTotalConnections() {
        return totalConnections;
    }


//    public Stats(int uptime, int threads, long getMisses, long getHits, long cmdSet, long cmdGet, long bytes) {
//        this.uptime = uptime;
//        this.threads = threads;
//        this.getMisses = getMisses;
//        this.getHits = getHits;
//        this.cmdSet = cmdSet;
//        this.cmdGet = cmdGet;
//        this.bytes = bytes;
//    }


}
