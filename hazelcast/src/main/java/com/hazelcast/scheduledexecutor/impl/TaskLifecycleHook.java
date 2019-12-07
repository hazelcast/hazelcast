package com.hazelcast.scheduledexecutor.impl;

public interface TaskLifecycleHook {

    default void preTaskSchedule(String scheduler, TaskDefinition definition) {};

    default void postTaskSchedule(String scheduler, TaskDefinition definition) {};

    default void preTaskDestroy(String scheduler, TaskDefinition definition) {};

    default void postTaskDestroy(String scheduler, TaskDefinition definition) {};
}
