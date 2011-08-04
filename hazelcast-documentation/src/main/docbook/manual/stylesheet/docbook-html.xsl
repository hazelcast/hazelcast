<?xml version="1.0" encoding="utf-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:fo="http://www.w3.org/1999/XSL/Format"
                xmlns:xslthl="http://xslthl.sf.net"
                exclude-result-prefixes="xslthl"
                version='1.0'>

    <!-- import the main stylesheet, here pointing to fo/docbook.xsl -->
    <xsl:import href="urn:docbkx:stylesheet"/>
    <!-- highlight.xsl must be imported in order to enable highlighting support, highlightSource=1 parameter
 is not sufficient -->
    <xsl:import href="urn:docbkx:stylesheet/highlight.xsl"/>

    <xsl:param name="highlight.source" select="1"/>
    <xsl:param name="admon.graphics" select="1"/>
    <xsl:param name="chapter.autolabel" select="1"/>
    <xsl:param name="section.autolabel" select="1"/>
    <xsl:param name="section.label.includes.component.label" select="1"/>
    <!-- some customization -->
    <!--
  <xsl:template match='xslthl:keyword' mode="xslthl">
   <font size="15" face="arial" color="red"><xsl:apply-templates mode="xslthl" /></font>
  </xsl:template>
    -->
    <!--
  <xsl:template match='xslthl:keyword' mode="xslthl">
      <font size="5" face="arial" color="red"><xsl:apply-templates mode="xslthl"/></font>
  </xsl:template>

  <xsl:template match='xslthl:comment'   mode="xslthl">
    <font font-style="italic" color="gray"><xsl:apply-templates  mode="xslthl"/></font>
  </xsl:template>

  <xsl:template match='xslthl:multiline-comment'   mode="xslthl">
    <font font-style="italic" color="gray"><xsl:apply-templates  mode="xslthl" /></font>
  </xsl:template>

  <xsl:template match='xslthl:string'   mode="xslthl">
    <font font-style="normal" color="#0000EE"><xsl:apply-templates  mode="xslthl"/></font>
  </xsl:template>

  <xsl:template match='xslthl:number'  mode="xslthl" >
    <font font-style="italic" color="#FF3311"><xsl:apply-templates  mode="xslthl"/></font>
  </xsl:template>

  <xsl:template match='xslthl:xml'  mode="xslthl" >
    <font font-style="italic" color="blue"><xsl:apply-templates  mode="xslthl"/></font>
  </xsl:template>  -->

    <!--
   <xsl:attribute-set name="monospace.verbatim.properties">
   <xsl:attribute name="font-family">Lucida Sans Typewriter</xsl:attribute>
     <xsl:attribute name="font-size">9pt</xsl:attribute>
     <xsl:attribute name="keep-together.within-column">always</xsl:attribute>
   </xsl:attribute-set>

   <xsl:param name="shade.verbatim" select="1"/>

   <xsl:attribute-set name="shade.verbatim.style">
     <xsl:attribute name="background-color">#f0f0f0</xsl:attribute>
     <xsl:attribute name="border-width">0.5pt</xsl:attribute>
     <xsl:attribute name="border-style">solid</xsl:attribute>
     <xsl:attribute name="border-color">#f0f0f0</xsl:attribute>
     <xsl:attribute name="padding">3pt</xsl:attribute>
   </xsl:attribute-set>
    -->

</xsl:stylesheet>