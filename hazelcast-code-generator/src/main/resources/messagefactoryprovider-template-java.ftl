package com.hazelcast.client.impl.protocol;

import com.hazelcast.instance.Node;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.client.impl.protocol.task.MessageTask;

public class ${model.className} implements MessageTaskFactoryProvider {

    private final MessageTaskFactory[] factories = new MessageTaskFactory[Short.MAX_VALUE];


    private final Node node;

    public  ${model.className} (NodeEngine nodeEngine) {
        this.node = ((NodeEngineImpl)nodeEngine).getNode();
        initFactories();
    }

    public void initFactories() {
<#assign package_keys = model.map?keys>
<#list package_keys as package_key>
//region ----------  REGISTRATION FOR ${package_key}
<#assign map = model.map[package_key]>
<#assign keys = map?keys>
<#list keys as key>
        factories[${key}.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ${map[key]}(clientMessage, node, connection);
            }
        };
</#list>
//endregion
</#list>

    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings({"MS_EXPOSE_REP", "EI_EXPOSE_REP"})
    @Override
    public MessageTaskFactory[] getFactories() {
        return factories;
    }
}


