package com.cis.utils;

import org.w3c.dom.*;

import javax.xml.parsers.*;
import javax.xml.xpath.*;

public class Xml {
    private Document doc;
    private Element rootElement;

    public final static StringBuffer PATH = new StringBuffer(Config.getInstance().getPath()).append("WEB-INF/fulltext.xml");

    public Xml(String fileName) {
        DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();
        domFactory.setNamespaceAware(true);
        try {
            DocumentBuilder builder = domFactory.newDocumentBuilder();
            this.doc = builder.parse(PATH.toString());
            this.rootElement = this.doc.getDocumentElement();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public NodeList findAll(String xpathStr) {
       return findAll(this.rootElement, xpathStr);
    }

    public Node find(String xpathStr) {
       NodeList nList = this.findAll(xpathStr);
       return nList == null? null: nList.item(0);
    }

    public static NodeList findAll(Node node, String xpathStr) {
        XPathFactory factory = XPathFactory.newInstance();
        XPath xpath = factory.newXPath();
        Object result = null;
        try {
            XPathExpression expr = xpath.compile(xpathStr);
            result = expr.evaluate(node, XPathConstants.NODESET);

        } catch(Exception e) {
            e.printStackTrace();
        }
        return (NodeList)result;
    }

    public static Node find(Node node, String xpathStr) {
        NodeList nList = findAll(node, xpathStr);
        return nList == null? null: nList.item(0);
    }

    public static String getAttributeValue(Node node,String name) {
        return node.getAttributes().getNamedItem(name).getTextContent();
    }

    public static void main(String[] args) {

    }
}
