package ${model.packageName};

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.BitUtil;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;

@edu.umd.cs.findbugs.annotations.SuppressWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public final class ${model.className} {


    public static final ${model.parentName}MessageType TYPE = ${model.parentName}MessageType.${model.parentName?upper_case}_${model.name?upper_case};
<#list model.params as param>
    public ${param.type} ${param.name};
</#list>

    private ${model.className}(ClientMessage clientMessage) {
<#list model.params as param>
        ${param.name} = clientMessage.${param.dataGetterString}();
</#list>
    }

    public static ${model.className} decode(ClientMessage clientMessage) {
        return new ${model.className}(clientMessage);
    }

    public static ClientMessage encode(<#list model.params as param>${param.type} ${param.name}<#if param_has_next>, </#if></#list>) {
        final int requiredDataSize = calculateDataSize(<#list model.params as param>${param.name}<#if param_has_next>, </#if></#list>);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.ensureCapacity(requiredDataSize);
        clientMessage.setMessageType(TYPE.id());
        <#list model.params as param>
        clientMessage.set(${param.name});
        </#list>
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    public static int calculateDataSize(<#list model.params as param>${param.type} ${param.name}<#if param_has_next>, </#if></#list>) {
        return ClientMessage.HEADER_SIZE
<#list model.params as param>
                + ${param.sizeString}<#if param_has_next>
</#if></#list>;
    }


}


