import {ClientMessage} from "../ClientMessage";
import {BitsUtil} from '../BitsUtil';
import {CustomCodec} from './CustomCodec';
import {${model.parentName}MessageType} from './${model.parentName}MessageType';

var REQUEST_TYPE = ${model.parentName}MessageType.${model.parentName?upper_case}_${model.name?upper_case}
var RESPONSE_TYPE = ${model.response}
var RETRYABLE = <#if model.retryable == 1>true<#else>false</#if>

<#--************************ REQUEST ********************************************************-->

export class ${model.className}{

constructor() {
}




static calculateSize(<#list model.requestParams as param>${util.convertToNodeType(param.name)}<#if param_has_next>, </#if></#list>){
    // Calculates the request payload size
    var dataSize = 0;
<#list model.requestParams as p>
    <@sizeText varName=util.convertToNodeType(p.name) type=p.type isNullable=p.nullable/>
</#list>
    return dataSize;
}

static encodeRequest(<#list model.requestParams as param>${util.convertToNodeType(param.name)}<#if param_has_next>, </#if></#list>){
    // Encode request into client_message
    var payloadSize;
    var clientMessage = new ClientMessage(payloadSize=calculateSize(<#list model.requestParams as param>${util.convertToNodeType(param.name)}<#if param_has_next>, </#if></#list>));
    clientMessage.setMessageType(REQUEST_TYPE);
    clientMessage.setRetryable(RETRYABLE);
<#list model.requestParams as p>
<@setterText varName=util.convertToNodeType(p.name) type=p.type isNullable=p.nullable/>
</#list>
    clientMessage.updateFrameLength();
    return clientMessage;
}

<#--************************ RESPONSE ********************************************************-->
static decodeResponse(clientMessage){
    // Decode response from client message
    var parameters;
<#list model.responseParams as p>
<@getterText varName=util.convertToNodeType(p.name) type=p.type isNullable=p.nullable indent=1/>
</#list>
    return parameters;

}

}
<#--************************ EVENTS ********************************************************-->
<#if model.events?has_content>
def handle(client_message, <#list model.events as event>handle_event_${event.name?lower_case} = None<#if event_has_next>, </#if></#list>):
    """ Event handler """
    message_type = client_message.get_message_type()
    <#list model.events as event>
    if message_type == EVENT_${event.name?upper_case} and handle_event_${event.name?lower_case} is not None:
        <#list event.eventParams as p>
<@getterText varName=util.convertToSnakeCase(p.name) type=p.type isNullable=p.nullable isEvent=true indent=2/>
        </#list>
        handle_event_${event.name?lower_case}(<#list event.eventParams as param>${util.convertToSnakeCase(param.name)}<#if param_has_next>, </#if></#list>)
    </#list>
</#if>

<#--MACROS BELOW-->
<#--SIZE NULL CHECK MACRO -->
<#macro sizeText varName type isNullable=false>
<#if isNullable>
    dataSize += BOOLEAN_SIZE_IN_BYTES;
    if ${varName} is not None:
<@sizeTextInternal varName=varName type=type indent=2/>
<#else>
<@sizeTextInternal varName=varName type=type indent=1/>
</#if>
</#macro>


<#--METHOD PARAM MACRO -->
<#macro methodParam type><#local cat= util.getTypeCategory(type)>
<#switch cat>
<#case "COLLECTION"><#local genericType= util.getGenericType(type)>java.util.Collection<${genericType}><#break>
<#default>${type}
</#switch>
</#macro>

<#--SIZE MACRO -->
<#macro sizeTextInternal varName type indent>
<#local cat= util.getTypeCategory(type)>
<#switch cat>
    <#case "OTHER">
        <#if util.isPrimitive(type)>
    dataSize += ${type?upper_case}_SIZE_IN_BYTES;
        <#else >
    dataSize += BitsUtil.calculateSize${util.capitalizeFirstLetter(util.getNodeType(type)?lower_case)}(${varName});
        </#if>
        <#break >
    <#case "CUSTOM">
    dataSize += BitsUtil.calculateSize${util.capitalizeFirstLetter(util.getNodeType(type)?lower_case)}(${varName});
        <#break >
    <#case "COLLECTION">
    dataSize += INT_SIZE_IN_BYTES;
        <#local genericType= util.getGenericType(type)>
        <#local n= varName>
${""?left_pad(indent * 4)}for ${varName}_item in ${varName}:
        <@sizeTextInternal varName="${n}_item"  type=genericType indent=(indent + 1)/>
        <#break >
    <#case "ARRAY">
${""?left_pad(indent * 4)}data_size += INT_SIZE_IN_BYTES
        <#local genericType= util.getArrayType(type)>
        <#local n= varName>
${""?left_pad(indent * 4)}for ${varName}_item in ${varName}:
        <@sizeTextInternal varName="${n}_item"  type=genericType indent=(indent + 1)/>
        <#break >
    <#case "MAP">
        <#local keyType = util.getFirstGenericParameterType(type)>
        <#local valueType = util.getSecondGenericParameterType(type)>
        <#local n= varName>
${""?left_pad(indent * 4)}for key, val in ${varName}.iteritems():
        <@sizeTextInternal varName="key"  type=keyType indent=(indent + 1)/>
        <@sizeTextInternal varName="val"  type=valueType indent=(indent + 1)/>
</#switch>
</#macro>

<#--SETTER NULL CHECK MACRO -->
<#macro setterText varName type isNullable=false>
<#local isNullVariableName= "${varName}_is_null">
<#if isNullable>
    clientMessage.appendBool(${varName} is None);
    if ${varName} is not None:
<@setterTextInternal varName=varName type=type indent=2/>
<#else>
<@setterTextInternal varName=varName type=type indent=1/>
</#if>
</#macro>

<#--SETTER MACRO -->
<#macro setterTextInternal varName type indent>
    <#local cat= util.getTypeCategory(type)>
    <#if cat == "OTHER">
    clientMessage.append${util.capitalizeFirstLetter(util.capitalizeFirstLetter(util.getNodeType(type)?lower_case))}(${varName});
    </#if>
    <#if cat == "CUSTOM">
    ${util.getTypeCodec(type)?split(".")?last}.encode(client_message, ${varName});
    </#if>
    <#if cat == "COLLECTION">
    clientMessage.appendInt(len(${varName}))
        <#local itemType= util.getGenericType(type)>
        <#local itemTypeVar= varName + "_item">
${""?left_pad(indent * 4)}for ${itemTypeVar} in ${varName}:
    <@setterTextInternal varName=itemTypeVar type=itemType indent=(indent + 1)/>
    </#if>
    <#if cat == "ARRAY">
${""?left_pad(indent * 4)}clientMessage.appendInt(len(${varName}));
        <#local itemType= util.getArrayType(type)>
        <#local itemTypeVar= varName + "_item">
${""?left_pad(indent * 4)}for ${itemTypeVar} in ${varName}:
    <@setterTextInternal varName=itemTypeVar  type=itemType indent=(indent + 1)/>
    </#if>
    <#if cat == "MAP">
        <#local keyType = util.getFirstGenericParameterType(type)>
        <#local valueType = util.getSecondGenericParameterType(type)>
${""?left_pad(indent * 4)}client_message.append_int(len(${varName}))
${""?left_pad(indent * 4)}for key, value in ${varName}.iteritems():
    <@setterTextInternal varName="key"  type=keyType indent=(indent + 1)/>
    <@setterTextInternal varName="val"  type=valueType indent=(indent + 1)/>
    </#if>
</#macro>

<#--GETTER NULL CHECK MACRO -->
<#macro getterText varName type isNullable=false isEvent=false indent=1>
<#if isNullable>
<#--${""?left_pad(indent * 4)}${varName}=None-->
if(clientMessage.readBool() === true){
<@getterTextInternal varName=varName varType=type isEvent=isEvent indent=indent +1/>
}
<#else>
<@getterTextInternal varName=varName varType=type isEvent=isEvent indent= indent/>
</#if>
</#macro>

<#macro getterTextInternal varName varType isEvent indent>
<#local cat= util.getTypeCategory(varType)>
<#switch cat>
    <#case "OTHER">
        <#switch varType>
            <#case util.DATA_FULL_NAME>
${""?left_pad(indent * 4)}<#if !isEvent>parameters['${varName}']<#else>${varName}</#if> = clientMessage.readData();
                <#break >
            <#case "java.lang.Integer">
${""?left_pad(indent * 4)}<#if !isEvent>parameters['${varName}']<#else>${varName}</#if> = clientMessage.readInt();
                <#break >
            <#case "java.lang.Boolean">
${""?left_pad(indent * 4)}<#if !isEvent>parameters['${varName}']<#else>${varName}</#if> = clientMessage.readBool();
                <#break >
            <#case "java.lang.String">
${""?left_pad(indent * 4)}<#if !isEvent>parameters['${varName}']<#else>${varName}</#if> = clientMessage.readStr();
                <#break >
            <#case "java.util.Map.Entry<com.hazelcast.nio.serialization.Data,com.hazelcast.nio.serialization.Data>">
${""?left_pad(indent * 4)}<#if !isEvent>parameters['${varName}']<#else>${varName}</#if> = clientMessage.readMapEntry();
                <#break >
            <#default>
${""?left_pad(indent * 4)}<#if !isEvent>parameters['${varName}']<#else>${varName}</#if> = clientMessage.read${util.capitalizeFirstLetter(util.getNodeType(varType))}();
        </#switch>
        <#break >
    <#case "CUSTOM">
${""?left_pad(indent * 4)}<#if !isEvent>parameters['${varName}']<#else>${varName}</#if> = ${util.getTypeCodec(varType)?split(".")?last}.decode(client_message)
        <#break >
    <#case "COLLECTION">
    <#case "ARRAY">
    <#local collectionType>java.util.ArrayList</#local>
    <#local itemVariableType= util.getGenericType(varType)>
    <#local itemVariableName= "${varName}_item">
    <#local sizeVariableName= "${varName}_size">
    <#local indexVariableName= "${varName}_index">
${""?left_pad(indent * 4)}${sizeVariableName} = client_message.read_int()
${""?left_pad(indent * 4)}${varName} = []
${""?left_pad(indent * 4)}for ${indexVariableName} in xrange(0, ${sizeVariableName}):
                            <@getterTextInternal varName=itemVariableName varType=itemVariableType isEvent=true indent=(indent +1)/>
${""?left_pad(indent * 4)}    ${varName}.append(${itemVariableName})
<#if !isEvent>
${""?left_pad(indent * 4)}parameters['${varName}'] = ${varName}
</#if>
        <#break >
    <#case "MAP">
        <#local sizeVariableName= "${varName}_size">
        <#local indexVariableName= "${varName}_index">
        <#local keyType = util.getFirstGenericParameterType(varType)>
        <#local valueType = util.getSecondGenericParameterType(varType)>
        <#local keyVariableName= "${varName}_key">
        <#local valVariableName= "${varName}_val">
${""?left_pad(indent * 4)}${sizeVariableName} = client_message.read_int()
${""?left_pad(indent * 4)}${varName} = {}
${""?left_pad(indent * 4)}for ${indexVariableName} in xrange(0,${sizeVariableName}):
            <@getterTextInternal varName=keyVariableName varType=keyType isEvent=true indent=(indent +1)/>
            <@getterTextInternal varName=valVariableName varType=valueType isEvent=true indent=(indent +1)/>
${""?left_pad(indent * 4)}    ${varName}[${keyVariableName}] = ${valVariableName}
${""?left_pad(indent * 4)}<#if !isEvent>parameters['${varName}'] = ${varName}</#if>
</#switch>
</#macro>