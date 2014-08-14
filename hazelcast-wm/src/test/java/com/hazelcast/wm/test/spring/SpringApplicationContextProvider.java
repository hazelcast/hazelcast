package com.hazelcast.wm.test.spring;

import org.eclipse.jetty.util.ConcurrentHashSet;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.util.Collections;
import java.util.Set;

public class SpringApplicationContextProvider implements ApplicationContextAware {

    private static Set<ApplicationContext> applicationContextSet =
                new ConcurrentHashSet<ApplicationContext>();

    public static Set<ApplicationContext> getApplicationContextSet() {
        return Collections.unmodifiableSet(applicationContextSet);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        applicationContextSet.add(applicationContext);
    }

}
