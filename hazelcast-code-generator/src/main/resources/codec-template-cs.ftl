using Hazelcast.Client.Protocol;
using Hazelcast.Client.Protocol.Util;
using Hazelcast.IO;
using Hazelcast.IO.Serialization;
using System.Collections.Generic;

namespace ${model.packageName}
{
    internal sealed class ${model.className}
    {

        public static readonly ${model.parentName}MessageType RequestType = ${model.parentName}MessageType.${model.parentName?cap_first}${model.name?cap_first};
        public const int ResponseType = ${model.response};
        public const bool Retryable = <#if model.retryable == 1>true<#else>false</#if>;

        //************************ REQUEST *************************//

        public class RequestParameters
        {
            public static readonly ${model.parentName}MessageType TYPE = RequestType;
    <#list model.requestParams as param>
            public ${util.getCSharpType(param.type)} ${param.name};
    </#list>

            public static int CalculateDataSize(<#list model.requestParams as param>${util.getCSharpType(param.type)} ${param.name}<#if param_has_next>, </#if></#list>)
            {
                int dataSize = ClientMessage.HeaderSize;
                <#list model.requestParams as p>
                    <@sizeText varName=p.name type=p.type isNullable=p.nullable/>
                </#list>
                return dataSize;
            }
        }

        public static ClientMessage EncodeRequest(<#list model.requestParams as param>${util.getCSharpType(param.type)} ${param.name}<#if param_has_next>, </#if></#list>)
        {
            int requiredDataSize = RequestParameters.CalculateDataSize(<#list model.requestParams as param>${param.name}<#if param_has_next>, </#if></#list>);
            ClientMessage clientMessage = ClientMessage.CreateForEncode(requiredDataSize);
            clientMessage.SetMessageType((int)RequestType);
            clientMessage.SetRetryable(Retryable);
            <#list model.requestParams as p>
                <@setterText varName=p.name type=p.type isNullable=p.nullable/>
            </#list>
            clientMessage.UpdateFrameLength();
            return clientMessage;
        }

        //************************ RESPONSE *************************//


        public class ResponseParameters
        {
            <#list model.responseParams as param>
            public ${util.getCSharpType(param.type)} ${param.name};
            </#list>
        }

        public static ResponseParameters DecodeResponse(IClientMessage clientMessage)
        {
            ResponseParameters parameters = new ResponseParameters();
            <#list model.responseParams as p>
                <@getterText varName=p.name type=p.type isNullable=p.nullable/>
            </#list>
            return parameters;
        }

    <#if model.events?has_content>

        //************************ EVENTS *************************//
        public abstract class AbstractEventHandler
        {
            public static void Handle(IClientMessage clientMessage, <#list model.events as event>Handle${event.name} handle${event.name}<#if event_has_next>, </#if></#list>)
            {
                int messageType = clientMessage.GetMessageType();
            <#list model.events as event>
                if (messageType == EventMessageConst.Event${event.name?cap_first}) {
                <#list event.eventParams as p>
                    <@getterText varName=p.name type=p.type isNullable=p.nullable isEvent=true/>
                </#list>
                    handle${event.name}(<#list event.eventParams as param>${param.name}<#if param_has_next>, </#if></#list>);
                    return;
                }
            </#list>
                Hazelcast.Logging.Logger.GetLogger(typeof(AbstractEventHandler)).Warning("Unknown message type received on event handler :" + clientMessage.GetMessageType());
            }

        <#list model.events as event>
            public delegate void Handle${event.name}(<#list event.eventParams as param>${util.getCSharpType(param.type)} ${param.name}<#if param_has_next>, </#if></#list>);
        </#list>
       }

    </#if>
    }
}
<#--MACROS BELOW-->
<#--SIZE NULL CHECK MACRO -->
<#macro sizeText varName type isNullable=false>
<#if isNullable>
                dataSize += Bits.BooleanSizeInBytes;
                if (${varName} != null)
                {
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
                dataSize += Bits.${type?cap_first}SizeInBytes;
        <#else >
                dataSize += ParameterUtil.CalculateDataSize(${varName});
        </#if>
        <#break >
    <#case "CUSTOM">
                dataSize += ${util.getTypeCodec(type)?split(".")?last}.CalculateDataSize(${varName});
        <#break >
    <#case "COLLECTION">
                dataSize += Bits.IntSizeInBytes;
        <#local genericType= util.getGenericType(type)>
        <#local n= varName>
                foreach (var ${varName}_item in ${varName} )
                {
                    <@sizeText varName="${n}_item" type=genericType/>
                }
        <#break >
    <#case "ARRAY">
                dataSize += Bits.IntSizeInBytes;
        <#local genericType= util.getArrayType(type)>
        <#local n= varName>
                foreach (var ${varName}_item in ${varName} ) {
                    <@sizeText varName="${n}_item"  type=genericType/>
                }
        <#break >
    <#case "MAP">
        <#local keyType = util.getFirstGenericParameterType(type)>
        <#local valueType = util.getSecondGenericParameterType(type)>
        <#local n= varName>
                foreach (var entry in ${varName}) {
                    var key = entry.Key;
                    var val = entry.Value;
                    <@sizeText varName="key"  type=keyType/>
                    <@sizeText varName="val"  type=valueType/>
                }
</#switch>
</#macro>

<#--SETTER NULL CHECK MACRO -->
<#macro setterText varName type isNullable=false>
<#local isNullVariableName= "${varName}_isNull">
<#if isNullable>
            bool ${isNullVariableName};
            if (${varName} == null)
            {
                ${isNullVariableName} = true;
                clientMessage.Set(${isNullVariableName});
            }
            else
            {
                ${isNullVariableName}= false;
                clientMessage.Set(${isNullVariableName});
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
            clientMessage.Set(${varName});
    </#if>
    <#if cat == "CUSTOM">
            ${util.getTypeCodec(type)?split(".")?last}.Encode(${varName}, clientMessage);
    </#if>
    <#if cat == "COLLECTION">
            clientMessage.Set(${varName}.Count);
            <#local itemType= util.getGenericType(type)>
            <#local itemTypeVar= varName + "_item">
            foreach (var ${itemTypeVar} in ${varName}) {
                <@setterTextInternal varName=itemTypeVar type=itemType/>
            }
    </#if>
    <#if cat == "ARRAY">
            clientMessage.Set(${varName}.length);
            <#local itemType= util.getArrayType(type)>
            <#local itemTypeVar= varName + "_item">
            foreach (var ${itemTypeVar} in ${varName}) {
                <@setterTextInternal varName=itemTypeVar  type=itemType/>
            }
    </#if>
    <#if cat == "MAP">
            <#local keyType = util.getFirstGenericParameterType(type)>
            <#local valueType = util.getSecondGenericParameterType(type)>
            clientMessage.Set(${varName}.Count);
            foreach (var entry in ${varName}) {
                var key = entry.Key;
                var val = entry.Value;
            <@setterTextInternal varName="key"  type=keyType/>
            <@setterTextInternal varName="val"  type=valueType/>
            }
    </#if>
</#macro>

<#--GETTER NULL CHECK MACRO -->
<#macro getterText varName type isNullable=false isEvent=false>
            ${util.getCSharpType(type)} ${varName} <#if !util.isPrimitive(type)>= null</#if>;
<#local isNullVariableName= "${varName}_isNull">
<#if isNullable>
            bool ${isNullVariableName} = clientMessage.GetBoolean();
            if (!${isNullVariableName})
            {
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
            ${varName} = clientMessage.GetData();
                <#break >
            <#case "java.lang.Integer">
            ${varName} = clientMessage.GetInt();
                <#break >
            <#case "java.lang.Boolean">
            ${varName} = clientMessage.GetBoolean();
                <#break >
            <#case "java.lang.String">
            ${varName} = clientMessage.GetStringUtf8();
                <#break >
            <#case "java.util.Map.Entry<com.hazelcast.nio.serialization.Data,com.hazelcast.nio.serialization.Data>">
            ${varName} = clientMessage.GetMapEntry();
                <#break >
            <#default>
            ${varName} = clientMessage.Get${util.capitalizeFirstLetter(varType)}();
        </#switch>
        <#break >
    <#case "CUSTOM">
            ${varName} = ${util.getTypeCodec(varType)?split(".")?last}.Decode(clientMessage);
        <#break >
    <#case "COLLECTION">
    <#local collectionType><#if varType?starts_with("java.util.List")>List<#else>HashSet</#if></#local>
    <#local itemVariableType= util.getGenericType(varType)>
    <#local convertedItemType= util.getCSharpType(itemVariableType)>
    <#local itemVariableName= "${varName}_item">
    <#local sizeVariableName= "${varName}_size">
    <#local indexVariableName= "${varName}_index">
            int ${sizeVariableName} = clientMessage.GetInt();
            ${varName} = new ${collectionType}<${convertedItemType}>(${itemVariableType.startsWith("List")?then(sizeVariableName,"")});
            for (int ${indexVariableName} = 0; ${indexVariableName}<${sizeVariableName}; ${indexVariableName}++) {
                ${convertedItemType} ${itemVariableName};
                <@getterTextInternal varName=itemVariableName varType=itemVariableType/>
                ${varName}.Add(${itemVariableName});
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
        <#local keyTypeCs = util.getCSharpType(keyType)>
        <#local valueType = util.getSecondGenericParameterType(varType)>
        <#local valueTypeCs = util.getCSharpType(valueType)>
        <#local keyVariableName= "${varName}_key">
        <#local valVariableName= "${varName}_val">
        int ${sizeVariableName} = clientMessage.GetInt();
        ${varName} = new Dictionary<${keyTypeCs},${valueTypeCs}>(${sizeVariableName});
        for (int ${indexVariableName} = 0;${indexVariableName}<${sizeVariableName};${indexVariableName}++) {
            ${keyTypeCs} ${keyVariableName};
            ${valueTypeCs} ${valVariableName};
            <@getterTextInternal varName=keyVariableName varType=keyType/>
            <@getterTextInternal varName=valVariableName varType=valueType/>
            ${varName}.Add(${keyVariableName}, ${valVariableName});
        }
</#switch>
</#macro>