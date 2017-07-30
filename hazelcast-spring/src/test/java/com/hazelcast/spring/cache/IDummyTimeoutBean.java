package com.hazelcast.spring.cache;

import org.springframework.cache.annotation.Cacheable;

public interface IDummyTimeoutBean {

    @Cacheable("delay150")
    Object getDelay150(String key);

    @Cacheable("delay50")
    Object getDelay50(String key);

    @Cacheable("delayNo")
    Object getDelayNo(String key);

    @Cacheable("delay100")
    String getDelay100(String key);
}