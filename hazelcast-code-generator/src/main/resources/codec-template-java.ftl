package ${model.packageName};

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;
import com.hazelcast.nio.Bits;

@edu.umd.cs.findbugs.annotations.SuppressWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public final class ${model.className} {

    public static final ${model.parentName}MessageType REQUEST_TYPE = ${model.parentName}MessageType.${model.parentName?upper_case}_${model.name?upper_case};
    public static final int RESPONSE_TYPE = ${model.response};
    public static final boolean RETRYABLE = <#if model.retryable == 1>true<#else>false</#if>;

    //************************ REQUEST *************************//

    public static class RequestParameters {
    public static final ${model.parentName}MessageType TYPE = REQUEST_TYPE;
<#list model.requestParams as param>
        public ${param.type} ${param.name};
</#list>

        public static int calculateDataSize(<#list model.requestParams as param>${param.type} ${param.name}<#if param_has_next>, </#if></#list>) {
            int dataSize = ClientMessage.HEADER_SIZE;
<#list model.requestParams as param>
            ${param.sizeString}<#if param_has_next>
</#if></#list>

            return dataSize;
        }
    }


    public static ClientMessage encodeRequest(<#list model.requestParams as param>${param.type} ${param.name}<#if param_has_next>, </#if></#list>) {
        final int requiredDataSize = RequestParameters.calculateDataSize(<#list model.requestParams as param>${param.name}<#if param_has_next>, </#if></#list>);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.setMessageType(REQUEST_TYPE.id());
        clientMessage.setRetryable(RETRYABLE);
<#list model.requestParams as param>
        ${param.dataSetterString}
</#list>
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    public static RequestParameters decodeRequest(ClientMessage clientMessage) {
        final RequestParameters parameters = new RequestParameters();
<#list model.requestParams as param>
        ${param.dataGetterString}
</#list>
        return parameters;
    }

    //************************ RESPONSE *************************//

    public static class ResponseParameters {
<#list model.responseParams as param>
        public ${param.type} ${param.name};
</#list>

        public static int calculateDataSize(<#list model.responseParams as param>${param.type} ${param.name}<#if param_has_next>, </#if></#list>) {
            int dataSize = ClientMessage.HEADER_SIZE;
<#list model.responseParams as param>
            ${param.sizeString}<#if param_has_next>
</#if></#list>

            return dataSize;
        }
    }

    public static ClientMessage encodeResponse(<#list model.responseParams as param>${param.type} ${param.name}<#if param_has_next>, </#if></#list>) {
        final int requiredDataSize = ResponseParameters.calculateDataSize(<#list model.responseParams as param>${param.name}<#if param_has_next>, </#if></#list>);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.setMessageType(RESPONSE_TYPE);
<#list model.responseParams as param>
        ${param.dataSetterString}
</#list>
        clientMessage.updateFrameLength();
        return clientMessage;

    }

    public static ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ResponseParameters parameters = new ResponseParameters();
<#list model.responseParams as param>
        ${param.dataGetterString}
</#list>

        return parameters;
    }


<#if model.events?has_content>

    //************************ EVENTS *************************//

<#list model.events as event>
    public static ClientMessage encode${event.name}Event(<#list event.eventParams as param>${param.type} ${param.name}<#if param_has_next>, </#if></#list>){
        int dataSize = ClientMessage.HEADER_SIZE;
    <#list event.eventParams as param>
        ${param.sizeString}<#if param_has_next>
    </#if></#list>;

        ClientMessage clientMessage = ClientMessage.createForEncode(dataSize);
        clientMessage.setMessageType(com.hazelcast.client.impl.protocol.EventMessageConst.${event.typeString});
        clientMessage.addFlag(ClientMessage.LISTENER_EVENT_FLAG);

    <#list event.eventParams as param>
        ${param.dataSetterString}
    </#list>
        clientMessage.updateFrameLength();
        return clientMessage;
    };

</#list>


  public static abstract class AbstractEventHandler{

        public void handle(ClientMessage clientMessage) {
            int messageType = clientMessage.getMessageType();
        <#list model.events as event>
            if (messageType == com.hazelcast.client.impl.protocol.EventMessageConst.${event.typeString}) {
            <#list event.eventParams as param>
                ${param.eventGetterString}
            </#list>
                handle(<#list event.eventParams as param>${param.name}<#if param_has_next>, </#if></#list>);
                return;
            }
        </#list>
            com.hazelcast.logging.Logger.getLogger(super.getClass()).warning("Unknown message type received on event handler :" + clientMessage.getMessageType());
        }

    <#list model.events as event>
        public abstract void handle(<#list event.eventParams as param>${param.type} ${param.name}<#if param_has_next>, </#if></#list>);

    </#list>
   }

</#if>
}



