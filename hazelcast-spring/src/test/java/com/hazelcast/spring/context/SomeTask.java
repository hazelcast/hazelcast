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

package com.hazelcast.spring.context;

import com.hazelcast.nio.DataSerializable;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * @mdogan 4/6/12
 */
@SpringAware
public class SomeTask implements Callable<Boolean>, ApplicationContextAware, DataSerializable {

    private transient ApplicationContext context;

    private transient SomeBean someBean;

    public Boolean call() throws Exception {
        return context != null && someBean != null;
    }

    public void setApplicationContext(final ApplicationContext applicationContext) throws BeansException {
        context = applicationContext;
    }

    @Autowired
    public void setSomeBean(final SomeBean someBean) {
        this.someBean = someBean;
    }

    public void writeData(final DataOutput out) throws IOException {

    }

    public void readData(final DataInput in) throws IOException {

    }
}
