package com.hazelcast.spring.cache;

import org.springframework.cache.annotation.Cacheable;

/**
 * @mdogan 4/3/12
 */
public interface IDummyBean {

    @Cacheable("name")
    String getName(int k);

    @Cacheable("city")
    String getCity(int k);

    @Cacheable("temp")
    Object getNull();
}
