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
 * XML��ʽ���ݷ�װ����ı�ʾ���ں�һ��org.omg.dom.Document����
 * <p>Title: Jiffy</p>
 * <p>Description: Runway Radius System</p>
 * <p>Copyright: Copyright (c) 2003</p>
 * <p>Company: Runway Corp.</p>
 * @author $Author: xiefei $
 * @version $Revision: 1.3 $ $Date: 2003/11/25 07:37:53 $
 */
public class XmlPackager {

    /**
     * ϵͳXML����ȱʡ��XML������.
     */
    protected DocumentBuilder _parser = null;

    /**
     * ʵ�ʱ���XML���ݽṹ�Ķ���.
     */
    protected org.w3c.dom.Document _document = null;
    protected XmlEntry _rootEntry = null;

    /**
     * ����һ���յ�XmlPackager ����.
     */
    public XmlPackager() {
        parser();
    }

    /**
     * ��һ���Ѿ���������XML Document����XmlPackager.
     * ���Ǵ�ʱ parser==null
     *
     * @param document �������Ƶ�Document����.
     */
    public XmlPackager(org.w3c.dom.Document document) {
        _document = document;
    }

    /**
     * ȡ��ϵͳXML����ʹ�õ�XML������.
     * ����һ��Apache Xerces Parser��
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
     * ��һ��XML��ʽ��ʾ���ַ���������ΪXML Document����
     *
     * @param xmlString һ��XML��ʽ���ַ�����
     * @return �����Ľ���ɹ����,һ����Ϊ���ݸ�ʽ�Ĵ�������SAXException����
     * @throws Exception A SAXException��
     */
    public void parseString(String xmlString)
            throws Exception
    {
        org.xml.sax.InputSource source = new InputSource(new StringReader(xmlString));

        _document = parser().parse(source);

        // ȷ��root���±�ȡ��
        _rootEntry = null;
    }

    /**
     * ��һ��XML��ʽ��ʾ���ַ���������ΪXML Document����.
     *
     * @param xmlFile һ��XML�ļ���
     * @return �����Ľ���ɹ����,һ����Ϊ���ݸ�ʽ�Ĵ�������SAXException����
     * @throws Exception A SAXException or IOException��
      */
    public void parseFile(String xmlFile)
            throws Exception
    {
        _document = parser().parse(new File(xmlFile));

        // ȷ��root���±�ȡ��
        _rootEntry = null;
    }

    /**
     * ȡ��XML Docuement.
     *
     * @return �ں���Document����
     */
    public org.w3c.dom.Document document() {
        return _document;
    }

    /**
     * ȡ��XML����ROOT������ELEMENT��ʾ,�Ժ�Ĳ�����ȫ����ͨ��XmlEntryִ�С�
     *
     * @return Document Element��
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
     * �����XML����ת����Ϊstring������XML����ʾ�������
     * һ�������ڲ��������á�
     *
     * @param doc ����ת����Document����һ����XmlPackager�ں�����
     * @return A XML formatted string�������������һ������String��
     */
    public String toString(Document doc) {

        //write to a string buffer.
        Writer writer = new StringWriter();
/*
        //A XML file with "ISO-8859-1" encoding, identing enabled.
        OutputFormat format = new OutputFormat(Method.XML, "ISO-8859-1", true);

        //serialize this document with specified wrtiter & format
        XMLSerializer xs = new XMLSerializer(writer, format);

        //�����Exception��̫���ܷ�������Ϊ�����ڴ������
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
