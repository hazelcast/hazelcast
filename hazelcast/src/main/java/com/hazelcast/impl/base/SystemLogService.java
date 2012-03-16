/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.impl.base;

import com.hazelcast.impl.Node;
import com.hazelcast.nio.Address;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

public class SystemLogService {

    public enum Level {
        CS_NONE,
        CS_EMPTY,
        CS_INFO,
        CS_TRACE
    }

    private final ConcurrentMap<CallKey, CallState> mapCallStates = new ConcurrentHashMap<CallKey, CallState>(100, 0.75f, 32);

    private final Queue<SystemLog> joinLogs = new LinkedBlockingQueue<SystemLog>(10000);

    private final Queue<SystemLog> connectionLogs = new LinkedBlockingQueue<SystemLog>(10000);

    private final Queue<SystemLog> migrationLogs = new LinkedBlockingQueue<SystemLog>(10000);

    private final Queue<SystemLog> nodeLogs = new LinkedBlockingQueue<SystemLog>(10000);

    private volatile Level currentLevel = Level.CS_TRACE;

    private final Node node;

    public SystemLogService(Node node) {
        this.node = node;
    }

    public List<String> getLogBundle3() {
        ArrayList<String> systemLogList = new ArrayList<String>();
        systemLogList.add("kljpokş");
        return systemLogList;
    }

    public List<SystemLogRecord> getLogBundle() {
//        SystemLogBundle logBundle = new SystemLogBundle();
        ArrayList<SystemLogRecord> systemLogList = new ArrayList<SystemLogRecord>();
        String node = this.node.getThisAddress().getHost() + ":" + this.node.getThisAddress().getPort();
        for (SystemLog log : joinLogs) {
            systemLogList.add(new SystemLogRecord(0L, node, log.getDate(), log.toString(), log.getType().toString()));
        }
        for (SystemLog log : nodeLogs) {
            systemLogList.add(new SystemLogRecord(0L, node, log.getDate(), log.toString(), log.getType().toString()));
        }
        for (SystemLog log : connectionLogs) {
            systemLogList.add(new SystemLogRecord(0L, node, log.getDate(), log.toString(), log.getType().toString()));
        }
        for (SystemLog log : migrationLogs) {
            systemLogList.add(new SystemLogRecord(0L, node, log.getDate(), log.toString(), log.getType().toString()));
        }
        for (CallState callState : mapCallStates.values()) {
            for (Object log : callState.getLogs()) {
                SystemLog systemLog = (SystemLog) log;
                systemLogList.add(new SystemLogRecord(callState.getCallId(), node, systemLog.getDate(), systemLog.toString(), systemLog.getType().toString()));
            }
        }

//        logBundle.setSystemLogList(systemLogList);

        return systemLogList;
    }


    public CallState getOrCreateCallState(long callId, Address callerAddress, int callerThreadId) {
        if (currentLevel == Level.CS_NONE) return null;
        CallKey callKey = new CallKey(callerAddress, callerThreadId);
        CallState callBefore = mapCallStates.get(callKey);
        if (callBefore == null) {
            CallState callStateNew = new CallState(callId, callerAddress, callerThreadId);
            mapCallStates.put(callKey, callStateNew);
            int callStatesCount = mapCallStates.size();
            if (callStatesCount > 10000) {
                String msg = " CallStates created! You might have too many threads accessing Hazelcast!";
                logNode(callStatesCount + msg);
            }
            return callStateNew;
        } else {
            if (callBefore.getCallId() != callId) {
                callBefore.reset(callId);
            }
            return callBefore;
        }
    }

    public CallState getCallState(Address callerAddress, int callerThreadId) {
        if (currentLevel == Level.CS_NONE) return null;
        return mapCallStates.get(new CallKey(callerAddress, callerThreadId));
    }

    public CallState getCallStateForCallId(long callId, Address callerAddress, int callerThreadId) {
        if (currentLevel == Level.CS_NONE) return null;
        CallState callState = mapCallStates.get(new CallKey(callerAddress, callerThreadId));
        if (callState != null && callState.getCallId() == callId) {
            return callState;
        }
        return null;
    }

    public void shutdown() {
        mapCallStates.clear();
        connectionLogs.clear();
        nodeLogs.clear();
        joinLogs.clear();
        migrationLogs.clear();
    }

    public String dump() {
        StringBuilder sb = new StringBuilder();
        Set<SystemLog> sorted = new TreeSet<SystemLog>(new Comparator<SystemLog>() {
            public int compare(SystemLog o1, SystemLog o2) {
                long thisVal = o1.date;
                long anotherVal = o2.date;
                return (thisVal < anotherVal ? -1 : (thisVal == anotherVal ? 0 : 1));
            }
        });
        sorted.addAll(joinLogs);
        sorted.addAll(nodeLogs);
        sorted.addAll(connectionLogs);
        sorted.addAll(migrationLogs);
        for (SystemLog systemLog : sorted) {
            sb.append(systemLog.getType().toString());
            sb.append(" - ");
            sb.append(new Date(systemLog.getDate()).toString());
            sb.append(" - ");
            sb.append(systemLog.toString());
            sb.append("\n");
        }
        for (CallState callState : mapCallStates.values()) {
            sb.append(callState.toString());
            sb.append("\n");
            for (Object log : callState.getLogs()) {
                SystemLog systemLog = (SystemLog) log;
                sb.append(systemLog.getType().toString());
                sb.append(" - ");
                sb.append(new Date(systemLog.getDate()).toString());
                sb.append(" - ");
                sb.append(systemLog.toString());
                sb.append("\n");
            }
        }
        sb.append(node.concurrentMapManager.getPartitionManager().toString());
        sb.append("\n");
        return sb.toString();
    }

    private void dumpToFile(String log) throws IOException {
        String fileName = "hazelcast-" + node.getThisAddress() + ".dump.txt";
        File file = new File(fileName);
        FileWriter fileWriter = new FileWriter(file);
        BufferedWriter out = new BufferedWriter(fileWriter);
        out.write(log);
        out.close();
    }

    public void logConnection(String str) {
        if (currentLevel != Level.CS_NONE) {
            SystemObjectLog systemLog = new SystemObjectLog(str);
            systemLog.setType(SystemLog.Type.CONNECTION);
            joinLogs.offer(systemLog);
        }
    }

    public void logPartition(String str) {
        if (currentLevel != Level.CS_NONE) {
            SystemObjectLog systemLog = new SystemObjectLog(str);
            systemLog.setType(SystemLog.Type.PARTITION);
            joinLogs.offer(systemLog);
        }
    }

    public void logNode(String str) {
        if (currentLevel != Level.CS_NONE) {
            SystemObjectLog systemLog = new SystemObjectLog(str);
            systemLog.setType(SystemLog.Type.NODE);
            joinLogs.offer(systemLog);
        }
    }

    public void logJoin(String str) {
        if (currentLevel != Level.CS_NONE) {
            SystemObjectLog systemLog = new SystemObjectLog(str);
            systemLog.setType(SystemLog.Type.JOIN);
            joinLogs.offer(systemLog);
        }
    }

    public void setLogLevel(Level level) {
        this.currentLevel = level;
    }

    public boolean shouldLog(Level level) {
        return currentLevel != Level.CS_NONE && currentLevel.ordinal() >= level.ordinal();
    }

    public boolean shouldTrace() {
        return currentLevel != Level.CS_NONE && currentLevel.ordinal() >= Level.CS_TRACE.ordinal();
    }

    public boolean shouldInfo() {
        return currentLevel != Level.CS_NONE && currentLevel.ordinal() >= Level.CS_INFO.ordinal();
    }

    public void info(CallStateAware callStateAware, SystemLog callStateLog) {
        logState(callStateAware, Level.CS_INFO, callStateLog);
    }

    public void trace(CallStateAware callStateAware, SystemLog callStateLog) {
        logState(callStateAware, Level.CS_TRACE, callStateLog);
    }

    public void info(CallStateAware callStateAware, String msg) {
        logObject(callStateAware, Level.CS_INFO, msg);
    }

    public void trace(CallStateAware callStateAware, String msg) {
        logObject(callStateAware, Level.CS_TRACE, msg);
    }

    public void info(CallStateAware callStateAware, String msg, Object arg1) {
        logState(callStateAware, Level.CS_INFO, new SystemArgsLog(msg, arg1));
    }

    public void info(CallStateAware callStateAware, String msg, Object arg1, Object arg2) {
        logState(callStateAware, Level.CS_INFO, new SystemArgsLog(msg, arg1, arg2));
    }

    public void trace(CallStateAware callStateAware, String msg, Object arg1) {
        logState(callStateAware, Level.CS_TRACE, new SystemArgsLog(msg, arg1));
    }

    public void trace(CallStateAware callStateAware, String msg, Object arg1, Object arg2) {
        logState(callStateAware, Level.CS_TRACE, new SystemArgsLog(msg, arg1, arg2));
    }

    public void logObject(CallStateAware callStateAware, Level level, Object obj) {
        if (currentLevel.ordinal() >= level.ordinal()) {
            if (callStateAware != null) {
                CallState callState = callStateAware.getCallState();
                if (callState != null) {
                    callState.logObject(obj);
                }
            }
        }
    }

    public void logState(CallStateAware callStateAware, Level level, SystemLog callStateLog) {
        if (currentLevel.ordinal() >= level.ordinal()) {
            if (callStateAware != null) {
                CallState callState = callStateAware.getCallState();
                if (callState != null) {
                    callState.log(callStateLog);
                }
            }
        }
    }
}
