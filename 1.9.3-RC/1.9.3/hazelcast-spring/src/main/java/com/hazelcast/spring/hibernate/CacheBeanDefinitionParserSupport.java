package com.hazelcast.spring.hibernate;

import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import com.hazelcast.config.AbstractXmlConfigHelper;

public abstract class CacheBeanDefinitionParserSupport extends AbstractBeanDefinitionParser {

    protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {
        final SpringXmlBuilder springXmlBuilder = new SpringXmlBuilder(parserContext);
        springXmlBuilder.handle(element);
        return springXmlBuilder.getBeanDefinition();
    }
    
    private class SpringXmlBuilder extends AbstractXmlConfigHelper {

        private BeanDefinitionBuilder builder;
        
        public SpringXmlBuilder(ParserContext parserContext) {
            this.builder = createBeanDefinitionBuilder();
        }

        public AbstractBeanDefinition getBeanDefinition() {
            return builder.getBeanDefinition();
        }

        public void handle(Element element) {
            final NamedNodeMap atts = element.getAttributes();
            String instanceRefName = "instance";
            if (atts != null) {
                for (int a = 0; a < atts.getLength(); a++) {
                    final Node att = atts.item(a);
                    final String name = att.getNodeName();
                    
                    if ("instance-ref".equals(name)) {
                    	instanceRefName = att.getNodeValue();
                    	break;
                    }
                }
            }
            this.builder.addConstructorArgReference(instanceRefName);
        }
    }
    
    protected abstract BeanDefinitionBuilder createBeanDefinitionBuilder();
}
