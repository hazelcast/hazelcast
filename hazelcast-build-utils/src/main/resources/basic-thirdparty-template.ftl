<#list licenseMap as e>
<#if e.getValue()?size != 0>
<#-- 
  Some dependencies are dual-licensed. 
  Hazelcast uses the dependencies under one of the licenses.
  The other license is usually a copy-left license, which only confuses our users, so we exclude it.
-->
<#if e.getKey() != "LGPL-2.1-or-later" && e.getKey() != "GPL2 w/ CPE" && e.getKey() != "GNU General Public License, version 2 (GPL2), with the classpath exception">
${e.getKey()}
<#list e.getValue() as a>
  ${a.name + ":" + a.version?trim}
</#list>
</#if>
</#if>
</#list>

