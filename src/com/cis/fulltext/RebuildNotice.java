package com.cis.fulltext;

import com.fb.common.util.Env;
import com.fb.db.AllIndexsInfo;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;

import java.io.*;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: luhang
 * Date: Oct 25, 2010
 * Time: 10:27:24 AM
 * To change this template use File | Settings | File Templates.
 */
public class RebuildNotice {
    public static void main(String[] args) throws Exception {
        String index = args[0];
        Env.homePath = "/cndata/recent_index/WEB-INF/";
        AllIndexsInfo.getInstance().loadIndexInfoFromDB();
        System.out.println(Env.homePath);
        String[] keywords = new String[4];
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("/cndata/recent_index/WEB-INF/notice")));
        String data = null;
        int i = 0;
        while ((data = br.readLine()) != null) {
            keywords[i++] = data;
        }
        br.close();
        for (i = 0; i < keywords.length; i++) {
            System.out.println(keywords[i]);
        }
        Index idx = new Index(index);
        IndexReader ir = idx.initIndexReader();
        Document doc;
        Document docU;

        IndexWriter iw = new IndexWriter("/mnt/hugedata/cndata/index/bid", new StandardAnalyzer(),!IndexReader.indexExists("/mnt/hugedata/cndata/index/bid"));
        IndexWriter iwU = new IndexWriter("/mnt/hugedata/cndata/index/union_bid", new StandardAnalyzer(),!IndexReader.indexExists("/mnt/hugedata/cndata/index/union_bid"));
        IndexSearcher is = new IndexSearcher("/mnt/hugedata/cndata/index/union_" + index);

        int count = Integer.parseInt(args[1]);

        int max =  ir.numDocs();
        if(args.length == 3) {
            max = Integer.parseInt(args[2]);
        }

        for (i = Integer.parseInt(args[1]); i < max; i++) {
            count++;
            try {
                doc = ir.document(i);
                String key = doc.get(idx.keyField);
                Term t = new Term(idx.ui.keyField , key);
                TermQuery tq = new TermQuery(t);

                Hits hits = is.search(tq);
                if(hits.length() == 0) {
                    System.out.println(key + " not found");
                    continue;
                }
                docU = hits.doc(0);
                String free = doc.get("by3");
                if(free == null) {
                    free = "0";
                }
                String type = "bidnotice";
                String title = doc.get("bidtitle");
                if (free.trim().equals("1")) {
                    type = "bidnotice_mf";
                } else {
                    if (title.indexOf(keywords[0]) >= 0 || title.indexOf(keywords[1]) >= 0 || title.indexOf(keywords[2]) >= 0) {
                        type = "bidnotice_bg";
                    } else if (title.indexOf(keywords[3]) >= 0) {
                        type = "bidnotice_yg";
                    }
                }

                String txt = TxtContent.get("bidnotice", key);
                String att =  Attachment.getContent(doc.get("fuj"));
                String content = txt + att;
                StringBuffer f = new StringBuffer(content);
                for (String field : idx.unionFields) {
                    f.append(doc.get(field));
                }
                String fieldValue = f.toString().replaceAll(" ", "");
                doc.add(new Field(idx.unionField, fieldValue, Field.Store.NO, Field.Index.TOKENIZED));
                docU.add(new Field(idx.ui.unionField, fieldValue, Field.Store.NO, Field.Index.TOKENIZED));
                docU.removeField("infotype");
                docU.add(new Field("infotype", type, Field.Store.YES, Field.Index.UN_TOKENIZED));
                //System.out.println(TxtContent.get("bidnotice", key));
                //System.out.println(Attachment.getContent(doc.get("fuj")));
                iw.addDocument(doc);
                iwU.addDocument(docU);
                System.out.println(count + ": seq-" + key + "  type-" + type + " attach:" + doc.get("fuj") + " txt:" + txt.length() + " att:" + att.length());
            } catch (java.lang.IllegalArgumentException e) {
                continue;
            }
        }
        ir.close();
        iw.close();
        iwU.close();
        is.close();
    }
}
