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

package com.hazelcast.spring.context;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import org.junit.Assert;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.Callable;

import static org.junit.Assert.assertEquals;

/**
 * @author mdogan 4/6/12
 */
@SpringAware(beanName = "someTask")
@Component("someTask")
@Scope("prototype")
public class SomeTask implements Callable<Long>, ApplicationContextAware, DataSerializable {

    private transient ApplicationContext context;

    private transient SomeBean someBean;

    public Long call() throws Exception {
        SomeBean bean = (SomeBean) context.getBean("someBean");
        assertEquals(bean, someBean);
        return bean.value;
    }

    public void setApplicationContext(final ApplicationContext applicationContext) throws BeansException {
        context = applicationContext;
    }

    @Autowired
    public void setSomeBean(final SomeBean someBean) {
        this.someBean = someBean;
    }

    public void writeData(final ObjectDataOutput out) throws IOException {

    }

    public void readData(final ObjectDataInput in) throws IOException {

    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof SomeTask)) return false;

        final SomeTask someTask = (SomeTask) o;

        if (context != null ? !context.equals(someTask.context) : someTask.context != null) return false;
        if (someBean != null ? !someBean.equals(someTask.someBean) : someTask.someBean != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = context != null ? context.hashCode() : 0;
        result = 31 * result + (someBean != null ? someBean.hashCode() : 0);
        return result;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SomeTask{");
        sb.append("context=").append(context);
        sb.append(", someBean=").append(someBean);
        sb.append('}');
        return sb.toString();
    }
}
