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
 * 在配置文件里面处于相同级别的一项配置，实际上是xml element的一个封装class。
 * 为不熟悉XML的用户提供基于xml格式配置选项的访问接口
 * <p>Title: Jiffy</p>
 * <p>Description: Runway Radius System</p>
 * <p>Copyright: Copyright (c) 2003</p>
 * <p>Company: Runway Corp.</p>
 * @author $Author: xiefei $
 * @version $Revision: 1.3 $ $Date: 2003/11/25 07:37:53 $
 */
public class XmlEntry implements Serializable{

    /**
     * 这项配置的xml element表示。
     */
    private org.w3c.dom.Element _xmlElement = null;

    /**
     * 使用指定的element建立配置对象。
     *
     * @param element 用这个element建立新的XmlEntry。
     */
    public XmlEntry(org.w3c.dom.Element element) {
        _xmlElement = element;
    }

    /**
     * 返回内部使用的XML Element对象，为高级用户使用。
     *
     * @return dom.Element对象引用。
     */
    public org.w3c.dom.Element element() {
        return _xmlElement;
    }

    /**
     * 取得这个配置项的tag name。
     *
     * @return Element TAG-NAME相当于&lt;ELEM attr="value" /&gt;里面的ELEM字段。
     */
    public String entryName() {
        return _xmlElement.getTagName();
    }

    /**
     * 取得指定属性的取值。
     * @param 属性名称，相当于&lt;ELEM attr="value" /&gt;里面的"attr"。
     * @return 属性的取值，相当于&lt;ELEM attr="value" /&gt;里面的"value"。
     */
    public String getAttribute(String attribName) {
        return _xmlElement.getAttribute(attribName);
    }

    /**
     * 询问是否有指定名称的属性。
     * @param 属性名称，相当于&lt;ELEM attr="value" /&gt;里面的"attr"。
     * @return 是否含有这个属性。
     */
    public boolean hasAttribute(String attribName) {
        return _xmlElement.hasAttribute(attribName);
    }

    /**
     * 取得所有属性的名称－数值对应表，并且以Name-Value对的方式返回。
         * @return 相当于&lt;ELEM attr1="value1" attr2="value2"/&gt;里面的所有name-value pair。
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
     * 取得子entry(Element)的对象，用于嵌套配置选项的读取。
     * @param tagName 遵从Java包名称的定义，以当前作为根路径。
     * 例如以下的XML结构
     * <pre>
     * &lt;document&gt;
     *   &lt;element&gt;
     *       &lt;entry attrib_name="attrib_value"/&gt;
     *   &lt;/element&gt;
     * &lt;/document&gt;
     * </pre>
     * 通过element.entry 就可以直接得到 &lt;entry&gt; 这个XML ELEMENT
     *
     * @return XmlEntry 的一个子项。
     */
    public XmlEntry getSubEntry(String tagName) {
        StringTokenizer token = new StringTokenizer(tagName, ".");
        Element tagElem = _xmlElement;

        while (token.hasMoreTokens()) {
            String part = token.nextToken();
            NodeList nodeList = tagElem.getElementsByTagName(part);
            tagElem = (Element) nodeList.item(0);
        }

        //没有找到子项，返回null
        if (tagElem == _xmlElement) {
            return null;
        }

        return new XmlEntry(tagElem);
    }

    /**
     * 找出直属指定名称的element。
     */
    public XmlEntry getChildEntry(String tagName) {

        //得到这个Element所有子节点。
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
     * 取得所有具有相同tag的子entry(Element)的对象列表。
     * list里面的每个对象都是Entry.
     * 这个函数不像 getSubEntry() 不能依次取得隔层的ELEMENT
     *
     * @param tagName sub-entry 的TAG。
     * @return 所有具有相同TAG的sub entry list。
     */
    public List getSubEntryList(String tagName) {
        ArrayList entryList = new ArrayList();

        //得到具有指定tag name的所有子节点
        NodeList nodeList = _xmlElement.getElementsByTagName(tagName);

        int count = nodeList.getLength();
        for (int i = 0; i < count; i++) {
            //遍历每个elemnt并且生成新的entry
            Element subEntry = (Element) nodeList.item(i);
            XmlEntry entry = new XmlEntry(subEntry);

            //加入subentry的列表
            entryList.add(entry);
        }

        return entryList;
    }

    public List getChildEntryList(String tagName) {
        ArrayList entryList = new ArrayList();

        //得到这个Element所有子节点。
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
     * 取得子entry(Element)包含的文字对象，用于嵌套配置选项的读取。
     * @param tagName 遵从Java包名称的定义，以当前作为根路径。
     * 例如以下的XML结构
     * <pre>
     * &lt;document&gt;
     *   &lt;element&gt;
         *       &lt;entry attrib_name="attrib_value"&gt; Element Contents &lt/entry&gt;
     *   &lt;/element&gt;
     * &lt;/document&gt;
     * </pre>
     * 通过element.entry 就可以直接得到 &lt;entry&gt; 这个XML ELEMENT。
     * 然后取得内涵的"Element Contents"文字对象。
     *
     * @return XmlEntry 包含的文字内容。
     */
    public String getTextContent(String tagName) {
        XmlEntry entry = getSubEntry(tagName);
        Node text = entry.element().getFirstChild();

        return text.getNodeValue();
    }

    /**
     * 对于相同名称的element列表，依据其中的属性名称取得某项属性。
     * <pre>
     * &lt;document&gt;
     *   &lt;element&gt;
     *       &lt;entry attrib_name1="attrib_value1"&gt; Element Contents &lt/entry&gt;
     *       &lt;entry attrib_name2="attrib_value2"&gt; Element Contents &lt/entry&gt;
     *       &lt;entry attrib_name3="attrib_value3"&gt; Element Contents &lt/entry&gt;
     *   &lt;/element&gt;
     * &lt;/document&gt;
     * </pre>
     * 通过attrib_name="attrib_name1", attrib_value="attrib_value1" 就可以直接头一个entry element 。
     *
     * @param Paranet Element，包含一系列具有相同tag的sub-element，这些sub-element只是属性内容不同。
     * @param attribName 查找的属性的名称。
     * @param attribValue 查找的属性的取值。
     *
     * @return 所查找的XmlEntry。
     */
    public XmlEntry getEntryByAttrib(String tagName, String attribName,
                                     String attribValue) {
        //得到具有指定tag name的所有子节点
        NodeList nodeList = _xmlElement.getElementsByTagName(tagName);

        int count = nodeList.getLength();
        for (int i = 0; i < count; i++) {
            //遍历每个elemnt并且生成新的entry
            Element subEntry = (Element) nodeList.item(i);

            //检查每个element的属性是否和条件匹配。
            if (subEntry.getAttribute(attribName).equals(attribValue)) {
                return new XmlEntry(subEntry);
            }
        }

        return null;
    }
}
