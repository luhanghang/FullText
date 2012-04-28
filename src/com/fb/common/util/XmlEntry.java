package com.fb.common.util;

import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

/**
 * �������ļ����洦����ͬ�����һ�����ã�ʵ������xml element��һ����װclass��
 * Ϊ����ϤXML���û��ṩ����xml��ʽ����ѡ��ķ��ʽӿ�
 * <p>Title: Jiffy</p>
 * <p>Description: Runway Radius System</p>
 * <p>Copyright: Copyright (c) 2003</p>
 * <p>Company: Runway Corp.</p>
 * @author $Author: xiefei $
 * @version $Revision: 1.3 $ $Date: 2003/11/25 07:37:53 $
 */
public class XmlEntry implements Serializable{

    /**
     * �������õ�xml element��ʾ��
     */
    private org.w3c.dom.Element _xmlElement = null;

    /**
     * ʹ��ָ����element�������ö���
     *
     * @param element �����element�����µ�XmlEntry��
     */
    public XmlEntry(org.w3c.dom.Element element) {
        _xmlElement = element;
    }

    /**
     * �����ڲ�ʹ�õ�XML Element����Ϊ�߼��û�ʹ�á�
     *
     * @return dom.Element�������á�
     */
    public org.w3c.dom.Element element() {
        return _xmlElement;
    }

    /**
     * ȡ������������tag name��
     *
     * @return Element TAG-NAME�൱��&lt;ELEM attr="value" /&gt;�����ELEM�ֶΡ�
     */
    public String entryName() {
        return _xmlElement.getTagName();
    }

    /**
     * ȡ��ָ�����Ե�ȡֵ��
     * @param �������ƣ��൱��&lt;ELEM attr="value" /&gt;�����"attr"��
     * @return ���Ե�ȡֵ���൱��&lt;ELEM attr="value" /&gt;�����"value"��
     */
    public String getAttribute(String attribName) {
        return _xmlElement.getAttribute(attribName);
    }

    /**
     * ѯ���Ƿ���ָ�����Ƶ����ԡ�
     * @param �������ƣ��൱��&lt;ELEM attr="value" /&gt;�����"attr"��
     * @return �Ƿ���������ԡ�
     */
    public boolean hasAttribute(String attribName) {
        return _xmlElement.hasAttribute(attribName);
    }

    /**
     * ȡ���������Ե����ƣ���ֵ��Ӧ��������Name-Value�Եķ�ʽ���ء�
         * @return �൱��&lt;ELEM attr1="value1" attr2="value2"/&gt;���������name-value pair��
     */
    public Properties getAttributeList() {
        Properties attribList = new Properties();

        NamedNodeMap attribs = _xmlElement.getAttributes();

        int count = attribs.getLength();
        for (int i = 0; i < count; i++) {
            Node aNode = attribs.item(i);
            attribList.setProperty(aNode.getNodeName(), aNode.getNodeValue());
        }

        return attribList;
    }

    /**
     * ȡ����entry(Element)�Ķ�������Ƕ������ѡ��Ķ�ȡ��
     * @param tagName ���Java�����ƵĶ��壬�Ե�ǰ��Ϊ��·����
     * �������µ�XML�ṹ
     * <pre>
     * &lt;document&gt;
     *   &lt;element&gt;
     *       &lt;entry attrib_name="attrib_value"/&gt;
     *   &lt;/element&gt;
     * &lt;/document&gt;
     * </pre>
     * ͨ��element.entry �Ϳ���ֱ�ӵõ� &lt;entry&gt; ���XML ELEMENT
     *
     * @return XmlEntry ��һ�����
     */
    public XmlEntry getSubEntry(String tagName) {
        StringTokenizer token = new StringTokenizer(tagName, ".");
        Element tagElem = _xmlElement;

        while (token.hasMoreTokens()) {
            String part = token.nextToken();
            NodeList nodeList = tagElem.getElementsByTagName(part);
            tagElem = (Element) nodeList.item(0);
        }

        //û���ҵ��������null
        if (tagElem == _xmlElement) {
            return null;
        }

        return new XmlEntry(tagElem);
    }

    /**
     * �ҳ�ֱ��ָ�����Ƶ�element��
     */
    public XmlEntry getChildEntry(String tagName) {

        //�õ����Element�����ӽڵ㡣
        NodeList children = _xmlElement.getChildNodes();
        int count = children.getLength();
        for (int i = 0; i < count; i++) {
            Node child = children.item(i);

            // It must be a Element Node.
            short nodeType = child.getNodeType();
            if (nodeType != org.w3c.dom.Node.ELEMENT_NODE) {
                continue;
            }

            String nodeName = child.getNodeName();
            if (nodeName.equals(tagName)) {
                return new XmlEntry( (Element) child);
            }
        }

        return null;
    }

    /**
     * ȡ�����о�����ͬtag����entry(Element)�Ķ����б�
     * list�����ÿ��������Entry.
     * ����������� getSubEntry() ��������ȡ�ø����ELEMENT
     *
     * @param tagName sub-entry ��TAG��
     * @return ���о�����ͬTAG��sub entry list��
     */
    public List getSubEntryList(String tagName) {
        ArrayList entryList = new ArrayList();

        //�õ�����ָ��tag name�������ӽڵ�
        NodeList nodeList = _xmlElement.getElementsByTagName(tagName);

        int count = nodeList.getLength();
        for (int i = 0; i < count; i++) {
            //����ÿ��elemnt���������µ�entry
            Element subEntry = (Element) nodeList.item(i);
            XmlEntry entry = new XmlEntry(subEntry);

            //����subentry���б�
            entryList.add(entry);
        }

        return entryList;
    }

    public List getChildEntryList(String tagName) {
        ArrayList entryList = new ArrayList();

        //�õ����Element�����ӽڵ㡣
        NodeList children = _xmlElement.getChildNodes();
        int count = children.getLength();
        for (int i = 0; i < count; i++) {
            Node child = children.item(i);

            // It must be a Element Node.
            short nodeType = child.getNodeType();
            if (nodeType != org.w3c.dom.Node.ELEMENT_NODE) {
                continue;
            }

            String nodeName = child.getNodeName();
            if (nodeName.equals(tagName)) {
                XmlEntry entry = new XmlEntry( (Element) child);
                entryList.add(entry);
            }
        }

        return entryList;
    }

    /**
     * ȡ����entry(Element)���������ֶ�������Ƕ������ѡ��Ķ�ȡ��
     * @param tagName ���Java�����ƵĶ��壬�Ե�ǰ��Ϊ��·����
     * �������µ�XML�ṹ
     * <pre>
     * &lt;document&gt;
     *   &lt;element&gt;
         *       &lt;entry attrib_name="attrib_value"&gt; Element Contents &lt/entry&gt;
     *   &lt;/element&gt;
     * &lt;/document&gt;
     * </pre>
     * ͨ��element.entry �Ϳ���ֱ�ӵõ� &lt;entry&gt; ���XML ELEMENT��
     * Ȼ��ȡ���ں���"Element Contents"���ֶ���
     *
     * @return XmlEntry �������������ݡ�
     */
    public String getTextContent(String tagName) {
        XmlEntry entry = getSubEntry(tagName);
        Node text = entry.element().getFirstChild();

        return text.getNodeValue();
    }

    /**
     * ������ͬ���Ƶ�element�б��������е���������ȡ��ĳ�����ԡ�
     * <pre>
     * &lt;document&gt;
     *   &lt;element&gt;
     *       &lt;entry attrib_name1="attrib_value1"&gt; Element Contents &lt/entry&gt;
     *       &lt;entry attrib_name2="attrib_value2"&gt; Element Contents &lt/entry&gt;
     *       &lt;entry attrib_name3="attrib_value3"&gt; Element Contents &lt/entry&gt;
     *   &lt;/element&gt;
     * &lt;/document&gt;
     * </pre>
     * ͨ��attrib_name="attrib_name1", attrib_value="attrib_value1" �Ϳ���ֱ��ͷһ��entry element ��
     *
     * @param Paranet Element������һϵ�о�����ͬtag��sub-element����Щsub-elementֻ���������ݲ�ͬ��
     * @param attribName ���ҵ����Ե����ơ�
     * @param attribValue ���ҵ����Ե�ȡֵ��
     *
     * @return �����ҵ�XmlEntry��
     */
    public XmlEntry getEntryByAttrib(String tagName, String attribName,
                                     String attribValue) {
        //�õ�����ָ��tag name�������ӽڵ�
        NodeList nodeList = _xmlElement.getElementsByTagName(tagName);

        int count = nodeList.getLength();
        for (int i = 0; i < count; i++) {
            //����ÿ��elemnt���������µ�entry
            Element subEntry = (Element) nodeList.item(i);

            //���ÿ��element�������Ƿ������ƥ�䡣
            if (subEntry.getAttribute(attribName).equals(attribValue)) {
                return new XmlEntry(subEntry);
            }
        }

        return null;
    }
}
