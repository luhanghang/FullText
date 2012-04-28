package com.cis.fulltext;

import com.fb.common.util.Env;
import com.fb.db.AllIndexsInfo;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Lu Hang
 * Date: 2010-11-18
 * Time: 21:40:52
 * To change this template use File | Settings | File Templates.
 */
public class SureUniqueUnion {
    static final int BATCH = 10000;

    public static void main(String[] args) throws Exception {
        String index = args[0];
        Env.homePath = "/cndata/cnindex";
        AllIndexsInfo.getInstance().loadIndexInfoFromDB();
        UnionIndex idx = new UnionIndex(index);

        int from = 0;
        if (args.length == 2)
            from = Integer.parseInt(args[1]);

        IndexReader ir = idx.initIndexReader();

        for (int i = from; i < ir.maxDoc(); i++) {
            if (ir.isDeleted(i)) {
                continue;
            }
            Document doc = ir.document(i);
            String id = doc.get(idx.keyField);

            int offset = 0;
            int j = i;
            while (offset < 5000 && j < ir.maxDoc() - 1) {
                j++;
                if (ir.isDeleted(j)) {
                    continue;
                }
                if(++offset % 100 == 0)
                    System.out.print(".");
                Document doc1 = ir.document(j);
                String id1 = doc1.get(idx.keyField);
                if (id1.equals(id)) {
                    System.out.println(i + "/" + j + ":" + id);
                    ir.deleteDocument(j);
                }
            }
            if(i % 100 == 0) System.out.print("*");
            if(i % 1000 == 0) {
                System.out.println("------------------------------" + i);
            }
        }

        System.out.println(ir.numDocs()  + ":" + ir.maxDoc());
        ir.close();
        System.out.println("done!");
    }
}
