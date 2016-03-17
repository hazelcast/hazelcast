/* tslint:disable */
import ClientMessage = require('../ClientMessage');
import ImmutableLazyDataList = require('./ImmutableLazyDataList');
import {BitsUtil} from '../BitsUtil';
import Address = require('../Address');
import {Data} from '../serialization/Data';
import {${model.parentName}MessageType} from './${model.parentName}MessageType';

var REQUEST_TYPE = ${model.parentName}MessageType.${model.parentName?upper_case}_${model.name?upper_case};
var RESPONSE_TYPE = ${model.response};
var RETRYABLE = <#if model.retryable == 1>true<#else>false</#if>;

<#--************************ REQUEST ********************************************************-->

export class ${model.className}{



static calculateSize(<#list model.requestParams as param>${util.convertToNodeType(param.name)} : ${util.getNodeTsType(param.type)} <#if param_has_next> , </#if></#list>){
    // Calculates the request payload size
    var dataSize : number = 0;
<#list model.requestParams as p>
    <@sizeText varName=util.convertToNodeType(p.name) type=p.type isNullable=p.nullable/>
</#list>
    return dataSize;
}

static encodeRequest(<#list model.requestParams as param>${util.convertToNodeType(param.name)} : ${util.getNodeTsType(param.type)}<#if param_has_next>, </#if></#list>){
    // Encode request into clientMessage
    var clientMessage = ClientMessage.newClientMessage(this.calculateSize(<#list model.requestParams as param>${util.convertToNodeType(param.name)}<#if param_has_next>, </#if></#list>));
    clientMessage.setMessageType(REQUEST_TYPE);
    clientMessage.setRetryable(RETRYABLE);
<#list model.requestParams as p>
<@setterText varName=util.convertToNodeType(p.name) type=p.type isNullable=p.nullable/>
</#list>
    clientMessage.updateFrameLength();
    return clientMessage;
}

<#--************************ RESPONSE ********************************************************-->
<#if model.responseParams?has_content>
static decodeResponse(clientMessage : ClientMessage,  toObjectFunction: (data: Data) => any = null){
    // Decode response from client message
    var parameters :any = { <#list model.responseParams as p>'${util.convertToNodeType(p.name)}' : null}<#if p_has_next>, </#if></#list> };
<#list model.responseParams as p>
<@getterText varName=util.convertToNodeType(p.name) type=p.type isNullable=p.nullable/>
</#list>
    return parameters;

}
<#else>
// Empty decodeResponse(ClientMessage), this message has no parameters to decode
</#if>

<#--************************ EVENTS ********************************************************-->
<#if model.events?has_content>
static handle(clientMessage : ClientMessage, <#list model.events as event>handleEvent${util.capitalizeFirstLetter(event.name?lower_case)} : any<#if event_has_next>, </#if></#list> ,toObjectFunction: (data: Data) => any = null){

    var messageType = clientMessage.getMessageType();
    <#list model.events as event>
    if ( messageType === BitsUtil.EVENT_${event.name?upper_case} && handleEvent${util.capitalizeFirstLetter(event.name?lower_case)} !== null) {
        <#list event.eventParams as p>
     var ${util.convertToNodeType(p.name)} : ${util.getNodeTsType(p.type)};
     <@getterText varName=util.convertToNodeType(p.name) type=p.type isNullable=p.nullable isEvent=true isDefined=true />
        </#list>
        handleEvent${util.capitalizeFirstLetter(util.convertToNodeType(event.name?lower_case))}(<#list event.eventParams as param>${util.convertToNodeType(param.name)}<#if param_has_next>, </#if></#list>);
    }
    </#list>
}
</#if>

}
<#--MACROS BELOW-->
<#--SIZE NULL CHECK MACRO -->
<#macro sizeText varName type isNullable=false>
<#if isNullable>
    dataSize += BitsUtil.BOOLEAN_SIZE_IN_BYTES;
    if(${varName} !== null) {
<@sizeTextInternal varName=varName type=type/>
    }
<#else>
<@sizeTextInternal varName=varName type=type/>
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
<#macro sizeTextInternal varName type>
<#local cat= util.getTypeCategory(type)>
<#switch cat>
    <#case "OTHER">
        <#if util.isPrimitive(type)>
    dataSize += BitsUtil.${type?upper_case}_SIZE_IN_BYTES;
        <#else >
    dataSize += BitsUtil.calculateSize${util.capitalizeFirstLetter(util.getNodeType(type)?lower_case)}(${varName});
        </#if>
        <#break >
    <#case "CUSTOM">
    dataSize += BitsUtil.calculateSize${util.capitalizeFirstLetter(util.getNodeType(type)?lower_case)}(${varName});
        <#break >
    <#case "COLLECTION">
    dataSize += BitsUtil.INT_SIZE_IN_BYTES;
        <#local genericType= util.getGenericType(type)>
        <#local n= varName>
    for( var ${varName}Item in ${varName}){
        <@sizeTextInternal varName="${n}Item"  type=genericType />
    }
        <#break >
    <#case "ARRAY">
    data_size += BitsUtil.INT_SIZE_IN_BYTES
        <#local genericType= util.getArrayType(type)>
        <#local n= varName>
    for(var ${varName}Item in ${varName}){
        <@sizeTextInternal varName="${n}Item"  type=genericType />
    }
        <#break >
    <#case "MAP">
        <#local keyType = util.getFirstGenericParameterType(type)>
        <#local valueType = util.getSecondGenericParameterType(type)>
        <#local n= varName>
    for(var entry in ${varName}){
        <@sizeTextInternal varName="entry.key"  type=keyType />
        <@sizeTextInternal varName="entry.val"  type=valueType />
    }
</#switch>
</#macro>

<#--SETTER NULL CHECK MACRO -->
<#macro setterText varName type isNullable=false>
<#local isNullVariableName= "${varName}IsNull">
<#if isNullable>
    clientMessage.appendBoolean(${varName} === null);
    if(${varName} !== null){
<@setterTextInternal varName=varName type=type />
    }
<#else>
<@setterTextInternal varName=varName type=type />
</#if>
</#macro>

<#--SETTER MACRO -->
<#macro setterTextInternal varName type >
    <#local cat= util.getTypeCategory(type)>
    <#if cat == "OTHER">
    clientMessage.append${util.capitalizeFirstLetter(util.capitalizeFirstLetter(util.getNodeType(type)?lower_case))}(${varName});
    </#if>
    <#if cat == "CUSTOM">
    ${util.getTypeCodec(type)?split(".")?last}.encode(clientMessage, ${varName});
    </#if>
    <#if cat == "COLLECTION">
    clientMessage.appendInt32(${varName}.length);
        <#local itemType= util.getGenericType(type)>
        <#local itemTypeVar= varName + "Item">
    for(var ${itemTypeVar} in ${varName}) {
    <@setterTextInternal varName=itemTypeVar type=itemType />
    }
    </#if>
    <#if cat == "ARRAY">
    clientMessage.appendInt32(${varName}.length);
        <#local itemType= util.getArrayType(type)>
        <#local itemTypeVar= varName + "Item">
    for(var ${itemTypeVar} in ${varName}){
    <@setterTextInternal varName=itemTypeVar  type=itemType />
    }
    </#if>
    <#if cat == "MAP">
        <#local keyType = util.getFirstGenericParameterType(type)>
        <#local valueType = util.getSecondGenericParameterType(type)>
    clientMessage.appendInt32(${varName}.length);
    for(var entry in ${varName}){
    <@setterTextInternal varName="entry.key"  type=keyType />
    <@setterTextInternal varName="entry.val"  type=valueType />
    }
    </#if>
</#macro>

<#--GETTER NULL CHECK MACRO -->
<#macro getterText varName type isNullable=false isEvent=false isDefined=false >
<#if isNullable>

if(clientMessage.readBoolean() !== true){
<@getterTextInternal varName=varName varType=type isEvent=isEvent isDefined=isDefined />
}
<#else>
<@getterTextInternal varName=varName varType=type isEvent=isEvent isDefined=isDefined />
</#if>
</#macro>

<#macro getterTextInternal varName varType isEvent isDefined=false isCollection=false>
<#local cat= util.getTypeCategory(varType)>
<#switch cat>
    <#case "OTHER">
        <#switch varType>
            <#case util.DATA_FULL_NAME>
    <#if !isEvent>parameters['${varName}']<#else>${varName}</#if> = toObjectFunction(clientMessage.readData());
                <#break >
            <#case "java.lang.Integer">
    <#if !isEvent>parameters['${varName}']<#else>${varName}</#if> = clientMessage.readInt32();
                <#break >
            <#case "java.lang.Boolean">
    <#if !isEvent>parameters['${varName}']<#else>${varName}</#if> = clientMessage.readBoolean();
                <#break >
            <#case "java.lang.String">
    <#if !isEvent>parameters['${varName}']<#else>${varName}</#if> = clientMessage.readString();
                <#break >
            <#case "java.util.Map.Entry<com.hazelcast.nio.serialization.Data,com.hazelcast.nio.serialization.Data>">
    <#if !isEvent>parameters['${varName}']<#else>${varName}</#if> = clientMessage.readMapEntry();
                <#break >
            <#default>
    <#if !isEvent>parameters['${varName}']<#else>${varName}</#if> = clientMessage.read${util.capitalizeFirstLetter(util.getNodeType(varType))}();
        </#switch>
        <#break >
    <#case "CUSTOM">
    <#if !isEvent>parameters['${varName}']<#else>${varName}</#if> = ${util.getTypeCodec(varType)?split(".")?last}.decode(clientMessage)
        <#break >
    <#case "COLLECTION">
    <#case "ARRAY">
    <#local collectionType>java.util.ArrayList</#local>
    <#local itemVariableType= util.getGenericType(varType)>
    <#local itemVariableName= "${varName}Item">
    <#local sizeVariableName= "${varName}Size">
    <#local indexVariableName= "${varName}Index">
    var ${sizeVariableName} = clientMessage.readInt32();
    <#if !isDefined>
    var ${varName} : any = [];
    <#else>${varName} = [];
    </#if>
    for(var ${indexVariableName} = 0 ;  ${indexVariableName} <= ${sizeVariableName} ; ${indexVariableName}++){
                            var <@getterTextInternal varName=itemVariableName varType=itemVariableType isEvent=true isCollection=true/>
        ${varName}.push(${itemVariableName})
    }
<#if !isEvent || isCollection>
    parameters['${varName}'] = new ImmutableLazyDataList(${varName}, toObjectFunction);
</#if>
        <#break >
    <#case "MAP">
        <#local sizeVariableName= "${varName}Size">
        <#local indexVariableName= "${varName}Index">
        <#local keyType = util.getFirstGenericParameterType(varType)>
        <#local valueType = util.getSecondGenericParameterType(varType)>
        <#local keyVariableName= "${varName}Key">
        <#local valVariableName= "${varName}Val">
    var ${sizeVariableName} = clientMessage.readInt();
    ${varName} = {}
    for ${indexVariableName} in xrange(0,${sizeVariableName}):
            <@getterTextInternal varName=keyVariableName varType=keyType isEvent=true />
            <@getterTextInternal varName=valVariableName varType=valueType isEvent=true />
        ${varName}[${keyVariableName}] = ${valVariableName}
        <#if !isEvent>parameters['${varName}'] = ${varName};</#if>
</#switch>
</#macro>