<#-- 
  Some dependencies are dual-licensed. 
  Hazelcast uses the dependencies under one of the licenses.
  The other licenses are usually a copy-left licenses, which only confuses our users, so we exclude them.
-->
<#assign skippedLicenses = [
    "LGPL-2.1-or-later",
    "GPL2 w/ CPE",
    "GNU General Public License, version 2 (GPL2), with the classpath exception",
    "Server Side Public License, v 1"
  ]>
<#list licenseMap as e>
<#if e.getValue()?size != 0>
<#if !skippedLicenses?seq_contains(e.getKey())>
${e.getKey()}
<#list e.getValue() as a>
  ${a.name + ":" + a.version?trim}
</#list>
</#if>
</#if>
</#list>

