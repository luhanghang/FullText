package com.cis.fulltext;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;

import javax.activation.FileDataSource;
import javax.jnlp.IntegrationService;

/**
 * Created by IntelliJ IDEA.
 * User: luhang
 * Date: 2/14/11
 * Time: 1:48 PM
 * To change this template use File | Settings | File Templates.
 */
public class RebuildUnionBidnotice {
    public static void main(String[] args) throws Exception {
        String path = "/mnt/hugedata/cndata/index/";
        path += args[0];
        System.out.println(path);
        IndexReader ir = IndexReader.open(path);
        IndexWriter iw = new IndexWriter(path + "_new", new StandardAnalyzer(), args.length == 1);
        IndexWriter iwu = new IndexWriter(path + "_new_union", new StandardAnalyzer(), args.length == 1);
        System.out.println(ir.numDocs() + ":" + ir.maxDoc());
        Document doc = null;
        Document newDocU = null;
        //title,1&number,1&org,1&owner,1&area,1&vocation,1&jointime,2&combined,3&uniseq,0

        int from = 0;
        if (args.length == 2) {
            from = Integer.parseInt(args[1]);
        }
        for (int i = from; i < ir.maxDoc(); i++) {
            if (!ir.isDeleted(i)) {
                doc = ir.document(i);

                StringBuffer uString = new StringBuffer();
                uString.append(doc.getField("bidtitle").stringValue());
                uString.append(doc.getField("bidder").stringValue());
                uString.append(doc.getField("bid_number").stringValue());
                uString.append(doc.getField("buyer").stringValue());

                String fuj = doc.getField("fuj").stringValue();
                if (fuj != null && fuj.length() > 1) {
                    uString.append(Attachment.getContent(fuj));
                }
                uString.append(TxtContent.get("bidnotice", doc.getField("uniseq").stringValue()));

                String fieldValue = uString.toString().replaceAll(" ", "");
                doc.removeField("notice_cc");
                doc.add(new Field("notice_cc", fieldValue, Field.Store.NO, Field.Index.TOKENIZED));
                iw.addDocument(doc);

                newDocU = new Document();
                Field f = new Field("title", doc.getField("bidtitle").stringValue(), Field.Store.YES, Field.Index.TOKENIZED);
                newDocU.add(f);
                f = new Field("number", doc.getField("bid_number").stringValue(), Field.Store.YES, Field.Index.TOKENIZED);
                newDocU.add(f);
                f = new Field("org", doc.getField("bidder").stringValue(), Field.Store.YES, Field.Index.TOKENIZED);
                newDocU.add(f);
                f = new Field("owner", doc.getField("buyer").stringValue(), Field.Store.YES, Field.Index.TOKENIZED);
                newDocU.add(f);
                f = new Field("area", doc.getField("area").stringValue(), Field.Store.YES, Field.Index.TOKENIZED);
                newDocU.add(f);
                f = new Field("vocation", doc.getField("vocation").stringValue(), Field.Store.YES, Field.Index.TOKENIZED);
                newDocU.add(f);
                f = new Field("jointime", doc.getField("jointime").stringValue(), Field.Store.YES, Field.Index.UN_TOKENIZED);
                newDocU.add(f);
                f = new Field("uniseq", doc.getField("uniseq").stringValue(), Field.Store.YES, Field.Index.UN_TOKENIZED);
                newDocU.add(f);
                String free = doc.getField("by3").stringValue();
                String type = "bidnotice";
                if (free.trim().equals("1")) {
                    type = "bidnotice_mf";
                } else {
                    String title = doc.getField("bidtitle").stringValue();
                    if (title.indexOf("变更") >= 0 || title.indexOf("更正") >= 0 || title.indexOf("修改公告") >= 0) {
                        type = "bidnotice_bg";
                    } else if (title.indexOf("预告") >= 0) {
                        type = "bidnotice_yg";
                    }
                }
                f = new Field("infotype", type, Field.Store.YES, Field.Index.UN_TOKENIZED);
                newDocU.add(f);

                f = new Field("combined", fieldValue, Field.Store.NO, Field.Index.TOKENIZED);
                newDocU.add(f);
                iwu.addDocument(newDocU);
                System.out.println(i + "/" + ir.maxDoc() + ":" + type);
            } else {
                System.out.println(i + "/" + ir.maxDoc() + " is deleted");
            }
        }
        ir.close();
        iw.close();
        iwu.close();
    }
}
