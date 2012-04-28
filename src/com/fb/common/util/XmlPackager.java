package com.fb.common.util;

import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;

/**
 * XML格式数据封装对象的表示，内含一个org.omg.dom.Document对象。
 * <p>Title: Jiffy</p>
 * <p>Description: Runway Radius System</p>
 * <p>Copyright: Copyright (c) 2003</p>
 * <p>Company: Runway Corp.</p>
 * @author $Author: xiefei $
 * @version $Revision: 1.3 $ $Date: 2003/11/25 07:37:53 $
 */
public class XmlPackager {

    /**
     * 系统XML处理缺省的XML解析器.
     */
    protected DocumentBuilder _parser = null;

    /**
     * 实际保存XML数据结构的对象.
     */
    protected org.w3c.dom.Document _document = null;
    protected XmlEntry _rootEntry = null;

    /**
     * 创建一个空的XmlPackager 对象.
     */
    public XmlPackager() {
        parser();
    }

    /**
     * 用一个已经解析过的XML Document创建XmlPackager.
     * 但是此时 parser==null
     *
     * @param document 用来复制的Document对象.
     */
    public XmlPackager(org.w3c.dom.Document document) {
        _document = document;
    }

    /**
     * 取得系统XML处理使用的XML解析器.
     * 这是一个Apache Xerces Parser。
     * @return An Apache Xerxes Parser.
     */
    public DocumentBuilder parser() {
        if (_parser==null) {
            try
            {
                DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

                factory.setNamespaceAware(true);

                _parser = factory.newDocumentBuilder();
            }
            catch(Exception e)
            {
                System.out.println("Create DocumentBuilder failure!");
                _parser = null;
            }
            //_parser = new DocumentBuilder();
        }

        return _parser;
    }

    /**
     * 将一个XML格式表示的字符串解析成为XML Document对象。
     *
     * @param xmlString 一个XML格式的字符串。
     * @return 解析的结果成功与否,一般因为数据格式的错误引起SAXException错误。
     * @throws Exception A SAXException。
     */
    public void parseString(String xmlString)
            throws Exception
    {
        org.xml.sax.InputSource source = new InputSource(new StringReader(xmlString));

        _document = parser().parse(source);

        // 确保root重新被取得
        _rootEntry = null;
    }

    /**
     * 将一个XML格式表示的字符串解析成为XML Document对象.
     *
     * @param xmlFile 一个XML文件。
     * @return 解析的结果成功与否,一般因为数据格式的错误引起SAXException错误。
     * @throws Exception A SAXException or IOException。
      */
    public void parseFile(String xmlFile)
            throws Exception
    {
        _document = parser().parse(new File(xmlFile));

        // 确保root重新被取得
        _rootEntry = null;
    }

    /**
     * 取得XML Docuement.
     *
     * @return 内含的Document对象。
     */
    public org.w3c.dom.Document document() {
        return _document;
    }

    /**
     * 取得XML对象ROOT顶级的ELEMENT表示,以后的操作完全可以通过XmlEntry执行。
     *
     * @return Document Element。
     */
    public XmlEntry root() {
        if (_document==null) {
            return null;
        }

        if (_rootEntry==null) {
            org.w3c.dom.Element rootElem = _document.getDocumentElement();
            _rootEntry = new XmlEntry(rootElem);
        }

        return _rootEntry;
    }

    /**
     * 将这个XML对象转换成为string，用于XML的显示、输出。
     * 一般用作内部函数调用。
     *
     * @param doc 用来转换的Document对象，一般是XmlPackager内含对象。
     * @return A XML formatted string，如果出错将返回一个“”String。
     */
    public String toString(Document doc) {

        //write to a string buffer.
        Writer writer = new StringWriter();
/*
        //A XML file with "ISO-8859-1" encoding, identing enabled.
        OutputFormat format = new OutputFormat(Method.XML, "ISO-8859-1", true);

        //serialize this document with specified wrtiter & format
        XMLSerializer xs = new XMLSerializer(writer, format);

        //这里的Exception不太可能发生，因为都是内存操作。
        try {
            xs.serialize(doc);
            writer.close();
        }
        catch (IOException e) {
            //LogManager log = AtiServerImpl.atiServer().getLogManager();
            //log.errorE(getClass().getName(), "toString(): ", e);

            return new String("");
        }
*/
        return writer.toString();
    }

    /**
     * call toString(Document) to implement this function
     */
    public String toString(){
        return toString(document());
    }
}
