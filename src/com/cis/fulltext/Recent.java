package com.cis.fulltext;

import com.cis.utils.Xml;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.*;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Recent {
    public static void main(String[] args) throws Exception {
        String[] keywords = new String[4];
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("/cndata/recent_index/WEB-INF/notice")));
        String data = null;
        int x = 0;
        while ((data = br.readLine()) != null) {
            keywords[x++] = data;
        }
        br.close();
        for (x = 0; x < keywords.length; x++) {
            System.out.println(keywords[x]);
        }

        String name = args[0];
        String from = args[1];
        String to = args[2];

        String keyField = "uniseq";
        if (name.equals("result")) {
            keyField = "unid";
        }
        Xml config = new Xml("fulltext.xml");
        Node item = config.find("//Item[@id='" + name + "']");

        String[] unionFields = null;
        Node ufNode = Xml.find(item, "UnionFields");
        if (ufNode != null) {
            unionFields = ufNode.getTextContent().split(",");
        }

        String timeField = "jointime";
        Node tfNode = Xml.find(item, "TimeField");
        if (tfNode != null) {
            timeField = tfNode.getTextContent().trim();
        }

        String unionField = "combined";
        Node uNode = Xml.find(item, "UnionField");
        if (uNode != null) {
            unionField = uNode.getTextContent().trim();
        }

        Node uiNode = Xml.find(item, "UnionIndex");
        Map<String, String> fieldMap = new HashMap<String, String>();
        NodeList fs = Xml.findAll(uiNode, "Map");
        for (int i = 0; i < fs.getLength(); i++) {
            fieldMap.put(Xml.getAttributeValue(fs.item(i), "this"), Xml.getAttributeValue(fs.item(i), "union"));
        }

        Document d;
        Document doc;
        Document docU;

        String path = "/mnt/hugedata/cndata/index/";
        String filename = name + "_" + from + "_" + to;
        IndexWriter iw = new IndexWriter(path + filename, new StandardAnalyzer(), true);
        IndexWriter iwU = new IndexWriter(path + "union_" + filename, new StandardAnalyzer(), true);

        String condition = timeField + ":[" + from + " TO " + to + "]";
        IndexSearcher is = new IndexSearcher(path + name);
        QueryParser parser = new QueryParser(keyField, new StandardAnalyzer());
        Query query = parser.parse(condition);
        Hits hits = is.search(query);
        int hitNum = hits.length();
        for (int n = 0; n < hitNum; n++) {
            System.out.print((n + 1) + "/" + hitNum + ":");
            d = hits.doc(n);
            doc = new Document();
            docU = new Document();

            List fields = d.getFields();
            for (Object fld : fields) {
                Field f = (Field) fld;
                doc.add(f);
                String ufield = fieldMap.get(f.name());
                if (ufield != null) {
                    docU.add(new Field(ufield, f.stringValue(), Field.Store.YES, f.isIndexed() ? Field.Index.TOKENIZED : Field.Index.UN_TOKENIZED));
                }
            }

            StringBuffer uf = new StringBuffer();
            for (String fn : unionFields) {
                if (d.get(fn) != null) uf.append(d.get(fn));
            }

            String content = TxtContent.get(name, d.get(keyField));
            uf.append(content);

            String fj = d.get("fuj");
            if (fj != null && !fj.equals("0")) {
                String att = Attachment.getContent(fj);
                System.out.println("fuj:" + fj + " length:" + att.length());
                uf.append(att);
            }

            System.out.print("content-length:" + content.length() + "/" + uf.length());
            doc.add(new Field(unionField, uf.toString(), Field.Store.NO, Field.Index.TOKENIZED));
            docU.add(new Field(fieldMap.get(unionField), uf.toString(), Field.Store.NO, Field.Index.TOKENIZED));

            iw.addDocument(doc);
            String type = name;
            if (type.equals("bidnotice")) {
                String free = d.get("by3");
                if (free != null && free.trim().equals("1")) {
                    type = "bidnotice_mf";
                } else {
                    String bt = d.get("bidtitle");

                    if (bt.indexOf(keywords[0]) >= 0 || bt.indexOf(keywords[1]) >= 0 || bt.indexOf(keywords[2]) >= 0) {
                        type = "bidnotice_bg";
                    } else if (bt.indexOf(keywords[3]) >= 0) {
                        type = "bidnotice_yg";
                    }
                }
            }
            System.out.println("type:" + type);

            docU.add(new Field("infotype", type, Field.Store.YES, Field.Index.UN_TOKENIZED));
            iwU.addDocument(docU);
        }
        iw.close();
        iwU.close();
        is.close();
    }
}
