package com.hazelcast.web;

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.FactoryIdHelper;

/**
 * Created by mesutcelik on 3/19/14.
 */
public class WebDataSerializerHook implements DataSerializerHook{


    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.WEB_DS_FACTORY, -1000);
    public static final int SESSION_ATTRIBUTE_ID = 1;


    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return new WebDataSerializableFactory();
    }
}
