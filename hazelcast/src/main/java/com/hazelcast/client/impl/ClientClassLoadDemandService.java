package com.hazelcast.client.impl;

import com.hazelcast.client.impl.ClientClassLoadDemandService.ClassLoadDemandEvent;
import com.hazelcast.client.impl.protocol.task.AddClassLoadDemandListenerMessageTask.ClassLoadDemandListener;
import com.hazelcast.core.Member;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.NodeEngine;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ClientClassLoadDemandService implements EventPublishingService<ClassLoadDemandEvent, ClassLoadDemandListener> {
    public static final String SERVICE_NAME = "hz:impl:clientClassLoadDemandService";
    private static final long CLIENT_CLASS_LOAD_TIMEOUT_SECONDS = 10;
    private static final long CLIENT_CLASS_LOAD_INTERVAL_SECONDS = 2;
    private final ConcurrentMap<String, CountDownLatch> classNameToLatch = new ConcurrentHashMap<String, CountDownLatch>();
    private final ConcurrentMap<String, byte[]> classNameToClassData = new ConcurrentHashMap<String, byte[]>();
    private final NodeEngine nodeEngine;
    private final Member thisMember;

    public ClientClassLoadDemandService(NodeEngine nodeEngine) {
        thisMember = nodeEngine.getLocalMember();
        this.nodeEngine = nodeEngine;

    }

    @Override
    public void dispatchEvent(ClassLoadDemandEvent event, ClassLoadDemandListener listener) {
        listener.handle(event);
    }

    public byte[] fetchBytecodeFromClient(String className) {
        EventService eventService = nodeEngine.getEventService();
        Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, SERVICE_NAME);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        CountDownLatch downLatch = classNameToLatch.putIfAbsent(className, countDownLatch);
        if (downLatch != null) {
            countDownLatch = downLatch;
        }
        ClassLoadDemandEvent event = new ClassLoadDemandEvent(thisMember, className);

        try {
            long now = System.currentTimeMillis();
            long end = now + TimeUnit.SECONDS.toMillis(CLIENT_CLASS_LOAD_TIMEOUT_SECONDS);
            while (end > now) {
                for (EventRegistration registration : registrations) {
                    eventService.publishEvent(SERVICE_NAME, registration, event, 0);
                }

                if (countDownLatch.await(CLIENT_CLASS_LOAD_INTERVAL_SECONDS, TimeUnit.SECONDS)) {
                    return classNameToClassData.remove(className);
                }

                now = System.currentTimeMillis();

            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return null;

    }

    public void putClassData(String className, byte[] data) {
        assert (classNameToClassData.get(className) == null);
        classNameToClassData.put(className, data);
        CountDownLatch latch = classNameToLatch.get(className);
        latch.countDown();
    }

    public static class ClassLoadDemandEvent {
        private final Member demandingMember;
        private final String demandedClassName;

        public Member getDemandingMember() {
            return demandingMember;
        }

        public String getDemandedClassName() {
            return demandedClassName;
        }

        ClassLoadDemandEvent(Member demandingMember, String demandedClassName) {
            this.demandingMember = demandingMember;
            this.demandedClassName = demandedClassName;
        }
    }


}
