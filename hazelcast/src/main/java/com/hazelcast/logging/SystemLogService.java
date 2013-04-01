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

package com.hazelcast.logging;

import com.hazelcast.instance.Node;
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
        NONE("none"),
        DEFAULT("default"),
        INFO("info"),
        TRACE("trace");

        private String value;

        public String getValue() {
            return value;
        }

        Level(String value) {
            this.value = value;
        }

        public static Level toLevel(String level) {
            if(level.equals("trace"))
                return Level.TRACE;
            else if(level.equals("default"))
                return Level.DEFAULT;
            else if(level.equals("info"))
                return Level.INFO;

            return Level.NONE;
        }
    }

    private final ConcurrentMap<CallKey, CallState> mapCallStates = new ConcurrentHashMap<CallKey, CallState>(100, 0.75f, 32);

    private final Queue<SystemLog> joinLogs = new LinkedBlockingQueue<SystemLog>(10000);

    private final Queue<SystemLog> connectionLogs = new LinkedBlockingQueue<SystemLog>(10000);

    private final Queue<SystemLog> partitionLogs = new LinkedBlockingQueue<SystemLog>(10000);

    private final Queue<SystemLog> nodeLogs = new LinkedBlockingQueue<SystemLog>(10000);

    private volatile Level currentLevel = Level.DEFAULT;

    private final Node node;

    private final boolean systemLogEnabled;

    public SystemLogService(Node node) {
        this.node = node;
        systemLogEnabled = node != null && node.groupProperties.SYSTEM_LOG_ENABLED.getBoolean();
    }

    public String getCurrentLevel() {
        return currentLevel.getValue();
    }

    public void setCurrentLevel(String level) {
        this.currentLevel = Level.toLevel(level);
    }

    public List<SystemLogRecord> getLogBundle() {
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
        for (SystemLog log : partitionLogs) {
            systemLogList.add(new SystemLogRecord(0L, node, log.getDate(), log.toString(), log.getType().toString()));
        }
        for (CallState callState : mapCallStates.values()) {
            for (Object log : callState.getLogs()) {
                SystemLog systemLog = (SystemLog) log;
                systemLogList.add(new SystemLogRecord(callState.getCallId(), node, systemLog.getDate(), systemLog.toString(), systemLog.getType().toString()));
            }
        }
        return systemLogList;
    }


    public CallState getOrCreateCallState(long callId, Address callerAddress, int callerThreadId) {
        if (!systemLogEnabled || currentLevel == Level.NONE || callerAddress == null) return null;
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
        if (!systemLogEnabled || currentLevel == Level.NONE) return null;
        return mapCallStates.get(new CallKey(callerAddress, callerThreadId));
    }

    public CallState getCallStateForCallId(long callId, Address callerAddress, int callerThreadId) {
        if (!systemLogEnabled || currentLevel == Level.NONE) return null;
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
        partitionLogs.clear();
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
        sorted.addAll(partitionLogs);
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
        }
        sb.append(node.partitionService.toString());
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
        if (systemLogEnabled && currentLevel != Level.NONE) {
            SystemObjectLog systemLog = new SystemObjectLog(str);
            systemLog.setType(SystemLog.Type.CONNECTION);
            connectionLogs.offer(systemLog);
        }
    }

    public void logPartition(String str) {
        if (systemLogEnabled && currentLevel != Level.NONE) {
            SystemObjectLog systemLog = new SystemObjectLog(str);
            systemLog.setType(SystemLog.Type.PARTITION);
            partitionLogs.offer(systemLog);
        }
    }

    public void logNode(String str) {
        if (systemLogEnabled && currentLevel != Level.NONE) {
            SystemObjectLog systemLog = new SystemObjectLog(str);
            systemLog.setType(SystemLog.Type.NODE);
            nodeLogs.offer(systemLog);
        }
    }

    public void logJoin(String str) {
        if (systemLogEnabled && currentLevel != Level.NONE) {
            SystemObjectLog systemLog = new SystemObjectLog(str);
            systemLog.setType(SystemLog.Type.JOIN);
            joinLogs.offer(systemLog);
        }
    }

    public boolean shouldLog(Level level) {
        return systemLogEnabled && currentLevel != Level.NONE && currentLevel.ordinal() >= level.ordinal();
    }

    public boolean shouldTrace() {
        return systemLogEnabled && currentLevel != Level.NONE && currentLevel.ordinal() >= Level.TRACE.ordinal();
    }

    public boolean shouldInfo() {
        return systemLogEnabled && currentLevel != Level.NONE && currentLevel.ordinal() >= Level.INFO.ordinal();
    }

    public void info(CallStateAware callStateAware, SystemLog callStateLog) {
        logState(callStateAware, Level.INFO, callStateLog);
    }

    public void trace(CallStateAware callStateAware, SystemLog callStateLog) {
        logState(callStateAware, Level.TRACE, callStateLog);
    }

    public void info(CallStateAware callStateAware, String msg) {
        logObject(callStateAware, Level.INFO, msg);
    }

    public void trace(CallStateAware callStateAware, String msg) {
        logObject(callStateAware, Level.TRACE, msg);
    }

    public void info(CallStateAware callStateAware, String msg, Object arg1) {
        logState(callStateAware, Level.INFO, new SystemArgsLog(msg, arg1));
    }

    public void info(CallStateAware callStateAware, String msg, Object arg1, Object arg2) {
        logState(callStateAware, Level.INFO, new SystemArgsLog(msg, arg1, arg2));
    }

    public void trace(CallStateAware callStateAware, String msg, Object arg1) {
        logState(callStateAware, Level.TRACE, new SystemArgsLog(msg, arg1));
    }

    public void trace(CallStateAware callStateAware, String msg, Object arg1, Object arg2) {
        logState(callStateAware, Level.TRACE, new SystemArgsLog(msg, arg1, arg2));
    }

    public void logObject(CallStateAware callStateAware, Level level, Object obj) {
        if (systemLogEnabled && currentLevel.ordinal() >= level.ordinal()) {
            if (callStateAware != null) {
                CallState callState = callStateAware.getCallState();
                if (callState != null) {
                    callState.logObject(obj);
                }
            }
        }
    }

    public void logState(CallStateAware callStateAware, Level level, SystemLog callStateLog) {
        if (systemLogEnabled && currentLevel.ordinal() >= level.ordinal()) {
            if (callStateAware != null) {
                CallState callState = callStateAware.getCallState();
                if (callState != null) {
                    callState.log(callStateLog);
                }
            }
        }
    }

}
