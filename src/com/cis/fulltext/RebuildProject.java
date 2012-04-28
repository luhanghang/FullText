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
public class RebuildProject {
    public static void main(String[] args) throws Exception {
        String index = "project";
        Env.homePath = "/cndata/cnindex/";
        AllIndexsInfo.getInstance().loadIndexInfoFromDB();
        System.out.println(Env.homePath);
        Index idx = new Index(index);
        IndexReader ir = idx.initIndexReader();
        Document doc;
        Document docU;
        IndexWriter iwU = new IndexWriter("/mnt/hugedata/cndata/index/union_proj", new StandardAnalyzer(), !IndexReader.indexExists("/mnt/hugedata/cndata/index/union_proj"));
        IndexSearcher is = new IndexSearcher("/mnt/hugedata/cndata/index/union_" + index);

        int count = Integer.parseInt(args[0]);
        for (int i = Integer.parseInt(args[0]); i < ir.numDocs(); i++) {
            count++;
            try {
                doc = ir.document(i);
                String key = doc.get(idx.keyField);
                Term t = new Term(idx.ui.keyField, key);
                TermQuery tq = new TermQuery(t);

                Hits hits = is.search(tq);
                if(hits.length() == 0) {
                    System.out.println(key + " not found");
                    continue;
                }
                docU = hits.doc(0);
                String sortid = doc.get("pro_sortid");
                String type = "project_dt";
                if(sortid.trim().equals("02")) {
                    type = "project_nj";
                } else if (sortid.trim().equals("03")) {
                    type = "project_zj";
                }
                String content = TxtContent.get("project", key);
                StringBuffer f = new StringBuffer(content);
                for (String field : idx.unionFields) {
                    f.append(doc.get(field));
                }
                String fieldValue = f.toString().replaceAll(" ", "");
                docU.add(new Field(idx.ui.unionField, fieldValue, Field.Store.NO, Field.Index.TOKENIZED));
                docU.removeField("infotype");
                docU.add(new Field("infotype", type, Field.Store.YES, Field.Index.UN_TOKENIZED));
                iwU.addDocument(docU);
                System.out.println(count + ": seq-" + key + "  type-" + type);
            } catch (java.lang.IllegalArgumentException e) {
                continue;
            }
        }
        ir.close();
        iwU.close();
        is.close();
    }
}
