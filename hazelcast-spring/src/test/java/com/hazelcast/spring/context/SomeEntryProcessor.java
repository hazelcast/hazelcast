package com.hazelcast.spring.context;

import com.hazelcast.map.AbstractEntryProcessor;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.util.Map;

/**
 * @ali 28/11/13
 */
@SpringAware
public class SomeEntryProcessor extends AbstractEntryProcessor implements ApplicationContextAware {

    private transient ApplicationContext context;

    public Object process(Map.Entry entry) {
        if (context == null ){
            return ">null";
        }
        return "notNull";
    }

    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.context = applicationContext;
    }
}
