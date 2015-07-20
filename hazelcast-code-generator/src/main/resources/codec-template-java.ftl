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
<#list model.requestParams as p>
    <@sizeText varName=p.name type=p.type isNullable=p.nullable/>
</#list>
            return dataSize;
        }
    }

    public static ClientMessage encodeRequest(<#list model.requestParams as param>${param.type} ${param.name}<#if param_has_next>, </#if></#list>) {
        final int requiredDataSize = RequestParameters.calculateDataSize(<#list model.requestParams as param>${param.name}<#if param_has_next>, </#if></#list>);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.setMessageType(REQUEST_TYPE.id());
        clientMessage.setRetryable(RETRYABLE);
<#list model.requestParams as p>
    <@setterText varName=p.name type=p.type isNullable=p.nullable/>
</#list>
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    public static RequestParameters decodeRequest(ClientMessage clientMessage) {
        final RequestParameters parameters = new RequestParameters();
<#list model.requestParams as p>
    <@getterText varName=p.name type=p.type isNullable=p.nullable/>
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
<#list model.responseParams as p>
    <@sizeText varName=p.name type=p.type isNullable=p.nullable/>
</#list>
            return dataSize;
        }
    }

    public static ClientMessage encodeResponse(<#list model.responseParams as param>${param.type} ${param.name}<#if param_has_next>, </#if></#list>) {
        final int requiredDataSize = ResponseParameters.calculateDataSize(<#list model.responseParams as param>${param.name}<#if param_has_next>, </#if></#list>);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.setMessageType(RESPONSE_TYPE);
<#list model.responseParams as p>
    <@setterText varName=p.name type=p.type isNullable=p.nullable/>
</#list>
        clientMessage.updateFrameLength();
        return clientMessage;

    }

    public static ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ResponseParameters parameters = new ResponseParameters();
<#list model.responseParams as p>
    <@getterText varName=p.name type=p.type isNullable=p.nullable/>
</#list>
        return parameters;
    }

<#if model.events?has_content>

    //************************ EVENTS *************************//

<#list model.events as event>
    public static ClientMessage encode${event.name}Event(<#list event.eventParams as param>${param.type} ${param.name}<#if param_has_next>, </#if></#list>){
        int dataSize = ClientMessage.HEADER_SIZE;
    <#list event.eventParams as p>
        <@sizeText varName=p.name type=p.type isNullable=p.nullable/>
    </#list>;

        ClientMessage clientMessage = ClientMessage.createForEncode(dataSize);
        clientMessage.setMessageType(com.hazelcast.client.impl.protocol.EventMessageConst.${event.typeString});
        clientMessage.addFlag(ClientMessage.LISTENER_EVENT_FLAG);

    <#list event.eventParams as p>
        <@setterText varName=p.name type=p.type isNullable=p.nullable/>
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
            <#list event.eventParams as p>
                <@getterText varName=p.name type=p.type isNullable=p.nullable isEvent=true/>
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
<#--MACROS BELOW-->
<#--SIZE NULL CHECK MACRO -->
<#macro sizeText varName type isNullable=false>
<#if isNullable>
            dataSize += Bits.BOOLEAN_SIZE_IN_BYTES;
            if (${varName} != null) {
</#if>
<@sizeTextInternal varName=varName type=type/>
<#if isNullable>
            }
</#if>
</#macro>

<#--SIZE MACRO -->
<#macro sizeTextInternal varName type>
<#local cat= util.getTypeCategory(type)>
<#switch cat>
    <#case "OTHER">
        <#if util.isPrimitive(type)>
            dataSize += Bits.${type?upper_case}_SIZE_IN_BYTES;
        <#else >
            dataSize += ParameterUtil.calculateDataSize(${varName});
        </#if>
        <#break >
    <#case "CUSTOM">
            dataSize += ${util.getTypeCodec(type)}.calculateDataSize(${varName});
        <#break >
    <#case "COLLECTION">
            dataSize += Bits.INT_SIZE_IN_BYTES;
        <#local genericType= util.getGenericType(type)>
        <#local n= varName>
            for (${genericType} ${varName}_item : ${varName} ) {
        <@sizeText varName="${n}_item"  type=genericType/>
            }
        <#break >
    <#case "ARRAY">
            dataSize += Bits.INT_SIZE_IN_BYTES;
        <#local genericType= util.getArrayType(type)>
        <#local n= varName>
            for (${genericType} ${varName}_item : ${varName} ) {
        <@sizeText varName="${n}_item"  type=genericType/>
            }
        <#break >
    <#case "MAP">
        <#local keyType = util.getFirstGenericParameterType(type)>
        <#local valueType = util.getSecondGenericParameterType(type)>
        <#local n= varName>
        for (java.util.Map.Entry<${keyType},${valueType}> entry : ${varName}.entrySet() ) {
            ${keyType} key = entry.getKey();
            ${valueType} val = entry.getValue();
        <@sizeText varName="key"  type=keyType/>
        <@sizeText varName="val"  type=keyType/>
        }
</#switch>
</#macro>

<#--SETTER NULL CHECK MACRO -->
<#macro setterText varName type isNullable=false>
<#local isNullVariableName= "${varName}_isNull">
<#if isNullable>
        boolean ${isNullVariableName};
        if (${varName} == null) {
            ${isNullVariableName} = true;
            clientMessage.set(${isNullVariableName});
        } else {
            ${isNullVariableName}= false;
            clientMessage.set(${isNullVariableName});
</#if>
<@setterTextInternal varName=varName type=type/>
<#if isNullable>
        }
</#if>
</#macro>

<#--SETTER MACRO -->
<#macro setterTextInternal varName type>
    <#local cat= util.getTypeCategory(type)>
    <#if cat == "OTHER">
        clientMessage.set(${varName});
    </#if>
    <#if cat == "CUSTOM">
        ${util.getTypeCodec(type)}.encode(${varName}, clientMessage);
    </#if>
    <#if cat == "COLLECTION">
        clientMessage.set(${varName}.size());
        <#local itemType= util.getGenericType(type)>
        <#local itemTypeVar= varName + "_item">
        for (${itemType} ${itemTypeVar} : ${varName}) {
        <@setterTextInternal varName=itemTypeVar type=itemType/>
        }
    </#if>
    <#if cat == "ARRAY">
        clientMessage.set(${varName}.length);
        <#local itemType= util.getArrayType(type)>
        <#local itemTypeVar= varName + "_item">
        for (${itemType} ${itemTypeVar} : ${varName}) {
        <@setterTextInternal varName=itemTypeVar  type=itemType/>
        }
    </#if>
    <#if cat == "MAP">
        <#local keyType = util.getFirstGenericParameterType(type)>
        <#local valueType = util.getSecondGenericParameterType(type)>
        clientMessage.set(${varName}.size());
        for (java.util.Map.Entry<${keyType},${valueType}> entry : ${varName}.entrySet() ) {
            ${keyType} key = entry.getKey();
            ${valueType} val = entry.getValue();
        <@setterTextInternal varName="key"  type=keyType/>
        <@setterTextInternal varName="val"  type=keyType/>
        }
    </#if>
</#macro>

<#--GETTER NULL CHECK MACRO -->
<#macro getterText varName type isNullable=false isEvent=false>
        ${type} ${varName} <#if !util.isPrimitive(type)>= null</#if>;
<#local isNullVariableName= "${varName}_isNull">
<#if isNullable>
        boolean ${isNullVariableName} = clientMessage.getBoolean();
        if (!${isNullVariableName}) {
</#if>
<@getterTextInternal varName=varName varType=type/>
<#if !isEvent>
            parameters.${varName} = ${varName};
</#if>
<#if isNullable>
        }
</#if>
</#macro>

<#macro getterTextInternal varName varType>
<#local cat= util.getTypeCategory(varType)>
<#switch cat>
    <#case "OTHER">
        <#switch varType>
            <#case util.DATA_FULL_NAME>
        ${varName} = clientMessage.getData();
                <#break >
            <#case "java.lang.Integer">
        ${varName} = clientMessage.getInt();
                <#break >
            <#case "java.lang.Boolean">
        ${varName} = clientMessage.getBoolean();
                <#break >
            <#case "java.lang.String">
        ${varName} = clientMessage.getStringUtf8();
                <#break >
            <#default>
        ${varName} = clientMessage.get${util.capitalizeFirstLetter(varType)}();
        </#switch>
        <#break >
    <#case "CUSTOM">
            ${varName} = ${util.getTypeCodec(varType)}.decode(clientMessage);
        <#break >
    <#case "COLLECTION">
    <#local collectionType><#if varType?starts_with("java.util.Set")>java.util.HashSet<#else></#if>java.util.ArrayList</#local>
    <#local itemVariableType= util.getGenericType(varType)>
    <#local itemVariableName= "${varName}_item">
    <#local sizeVariableName= "${varName}_size">
    <#local indexVariableName= "${varName}_index">
            int ${sizeVariableName} = clientMessage.getInt();
            ${varName} = new ${collectionType}<${itemVariableType}>(${sizeVariableName});
            for (int ${indexVariableName} = 0;${indexVariableName}<${sizeVariableName};${indexVariableName}++) {
                ${itemVariableType} ${itemVariableName};
                <@getterTextInternal varName=itemVariableName varType=itemVariableType/>
                ${varName}.add(${itemVariableName});
            }
        <#break >
    <#case "ARRAY">
    <#local itemVariableType= util.getArrayType(varType)>
    <#local itemVariableName= "${varName}_item">
    <#local sizeVariableName= "${varName}_size">
    <#local indexVariableName= "${varName}_index">
            int ${sizeVariableName} = clientMessage.getInt();
            ${varName} = new ${itemVariableType}[${sizeVariableName}];
            for (int ${indexVariableName} = 0;${indexVariableName}<${sizeVariableName};${indexVariableName}++) {
                ${itemVariableType} ${itemVariableName};
                <@getterTextInternal varName=itemVariableName varType=itemVariableType/>
                ${varName}[${indexVariableName}] = ${itemVariableName};
            }
        <#break >
    <#case "MAP">
        <#local sizeVariableName= "${varName}_size">
        <#local indexVariableName= "${varName}_index">
        <#local keyType = util.getFirstGenericParameterType(varType)>
        <#local valueType = util.getSecondGenericParameterType(varType)>
        <#local keyVariableName= "${varName}_key">
        <#local valVariableName= "${varName}_val">
        int ${sizeVariableName} = clientMessage.getInt();
        ${varName} = new java.util.HashMap<${keyType},${valueType}>(${sizeVariableName});
        for (int ${indexVariableName} = 0;${indexVariableName}<${sizeVariableName};${indexVariableName}++) {
            ${keyType} ${keyVariableName};
            ${valueType} ${valVariableName};
            <@getterTextInternal varName=keyVariableName varType=keyType/>
            <@getterTextInternal varName=valVariableName varType=valueType/>
            ${varName}.put(${keyVariableName}, ${valVariableName});
        }
</#switch>
</#macro>