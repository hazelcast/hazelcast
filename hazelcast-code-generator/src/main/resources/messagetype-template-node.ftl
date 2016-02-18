/* tslint:disable:no-bitwise */
export class ${model.className} {
<#list model.params as param>
static ${model.name?upper_case}_${param.name?upper_case} = ${param.id};
</#list>
}