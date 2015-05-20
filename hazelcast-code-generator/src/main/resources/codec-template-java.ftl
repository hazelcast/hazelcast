package ${model.packageName};

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;
import com.hazelcast.nio.Bits;

@edu.umd.cs.findbugs.annotations.SuppressWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public final class ${model.className} {

    public static final ${model.parentName}MessageType REQUEST_TYPE = ${model.parentName}MessageType.${model.parentName?upper_case}_${model.name?upper_case};
    public static final short RESPONSE_TYPE = ${model.response};

    public static class RequestParameters {
<#list model.requestParams as param>
        public ${param.type} ${param.name};
</#list>

        public static int calculateDataSize(<#list model.requestParams as param>${param.type} ${param.name}<#if param_has_next>, </#if></#list>) {
            return ClientMessage.HEADER_SIZE
<#list model.requestParams as param>
                    + ${param.sizeString}<#if param_has_next>
</#if></#list>;
        }
    }

    public static class ResponseParameters {
<#list model.responseParams as param>
        public ${param.type} ${param.name};
</#list>

        public static int calculateDataSize(<#list model.responseParams as param>${param.type} ${param.name}<#if param_has_next>, </#if></#list>) {
            return ClientMessage.HEADER_SIZE
<#list model.responseParams as param>
                    + ${param.sizeString}<#if param_has_next>
</#if></#list>;
        }
    }

    public static ClientMessage encodeRequest(<#list model.requestParams as param>${param.type} ${param.name}<#if param_has_next>, </#if></#list>) {
        final int requiredDataSize = RequestParameters.calculateDataSize(<#list model.requestParams as param>${param.name}<#if param_has_next>, </#if></#list>);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.setMessageType(REQUEST_TYPE.id());
<#list model.requestParams as param>
        clientMessage.set(${param.name});
</#list>
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    public static RequestParameters decodeRequest(ClientMessage clientMessage) {
        final RequestParameters parameters = new RequestParameters();
<#list model.requestParams as param>
        parameters.${param.name} = clientMessage.${param.dataGetterString}();
</#list>
        return parameters;
    }

    public static ClientMessage encodeResponse(<#list model.responseParams as param>${param.type} ${param.name}<#if param_has_next>, </#if></#list>) {
        final int requiredDataSize = ResponseParameters.calculateDataSize(<#list model.responseParams as param>${param.name}<#if param_has_next>, </#if></#list>);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.setMessageType(RESPONSE_TYPE);
<#list model.responseParams as param>
        clientMessage.set(${param.name});
</#list>
        clientMessage.updateFrameLength();
        return clientMessage;

    }

    public static ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ResponseParameters parameters = new ResponseParameters();
<#list model.responseParams as param>
        parameters.${param.name} = clientMessage.${param.dataGetterString}();
</#list>
        return parameters;
    }

    public static boolean RETRYABLE = <#if model.retryable == 1>true<#else>false</#if>;

}



