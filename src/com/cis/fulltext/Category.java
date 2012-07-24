package com.cis.fulltext;

import com.fb.common.util.Env;
import com.fb.db.AllIndexsInfo;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class Category {
    private List<Record> category = new ArrayList<Record>();
    private Connection conn;
    private Statement state;
    private ResultSet rs;

    private Connection conn1;
    private Statement state1;
    private ResultSet rs1;
    private Index bi;
    private String source;
    public String content_name;


    public Category(String source) {
        this.source = source;
        this.content_name = source;
        init();
    }

    private void init() {
        String sql = "select code, keywords from category";
        try {
            init_conn("localhost", "infoseek", "root", "zzwl0518");
            rs = state.executeQuery(sql);
            rs.beforeFirst();
            while (rs.next()) {
                category.add(new Record(rs.getString("code"), rs.getString("keywords")));
            }
            init_conn1("localhost", "contents", "root", "zzwl0518");
            bi = new Index(source);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public List<Record> getCategory() {
        return this.category;
    }

    public String getCategory(String content) {
        StringBuffer c = new StringBuffer();
        List<Record> list = this.getCategory();
        for (Record r : list) {
            String[] kws = r.keywords.split(",");
            for (String k : kws) {
                if (!k.trim().equals("")) {
                    if (content.indexOf(k) >= 0) {
                        c.append(r.code).append(" ");
                        break;
                    }
                }
            }
        }
        return c.toString();
    }

    public void match(String out, int from) throws Exception {
        IndexWriter iw = new IndexWriter(out, new StandardAnalyzer(), true);
        iw.setMaxFieldLength(100000);
        long b = System.currentTimeMillis();
        List<Record> list = this.getCategory();
        try {
            IndexReader reader = IndexReader.open(bi.indexPath);
            Document doc;

            for (int i = from; i < reader.maxDoc(); i++) {
                try {
                    doc = reader.document(i);
                }
                catch (Exception e) {
                    e.printStackTrace();
                    continue;
                }
                String seq = doc.get(bi.keyField);

                String content = null;
                rs1 = state1.executeQuery("select content from " + content_name + " where seq='" + seq + "'");
                if (rs1.next()) {
                    content = rs1.getString("content");
                }
                rs1.close();
                if (content != null) {
                    String c = this.getCategory(content);
                    System.out.println(i + ":" + c);


//                    new_doc.add(new Field("uniseq", seq, Field.Store.YES, Field.Index.UN_TOKENIZED));
//                    new_doc.add(new Field("bidtitle", doc.get("bidtitle"), Field.Store.YES, Field.Index.TOKENIZED));
//                    new_doc.add(new Field("vocation", doc.get("vocation"), Field.Store.YES, Field.Index.TOKENIZED));
//                    new_doc.add(new Field("area", doc.get("area"), Field.Store.YES, Field.Index.TOKENIZED));
//                    new_doc.add(new Field("jointime", doc.get("jointime"), Field.Store.YES, Field.Index.UN_TOKENIZED));

                    StringBuffer f = new StringBuffer();
                    for (String field : bi.unionFields) {
                        if (doc.getField(field) != null)
                            f.append(doc.getField(field).stringValue());
                    }
                    f.append(content);
                    String fieldValue = f.toString().replaceAll(" ", "");
                    doc.add(new Field(bi.unionField, fieldValue, Field.Store.NO, Field.Index.TOKENIZED));
                    doc.add(new Field("cate", c, Field.Store.YES, Field.Index.TOKENIZED));
                    iw.addDocument(doc);
                }
            }
            reader.close();
            iw.close();
            long e = System.currentTimeMillis();
            System.out.println((e - b) / 1000 + " seconds used");
        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void init_conn(String host, String db, String user, String passwd) throws Exception {
        StringBuffer url = new StringBuffer("jdbc:mysql://").append(host).append("/").append(db).append("?useUnicode=true&characterEncoding=gb2312");
        Class.forName("com.mysql.jdbc.Driver").newInstance();
        this.conn = DriverManager.getConnection(url.toString(), user, passwd);
        this.state = this.conn.createStatement();
    }

    public void init_conn1(String host, String db, String user, String passwd) throws Exception {
        StringBuffer url = new StringBuffer("jdbc:mysql://").append(host).append("/").append(db).append("?useUnicode=true&characterEncoding=gb2312");
        Class.forName("com.mysql.jdbc.Driver").newInstance();
        this.conn1 = DriverManager.getConnection(url.toString(), user, passwd);
        this.state1 = this.conn1.createStatement();
    }

    public static void main(String[] args) throws Exception {
        Env.homePath = args[0];
        AllIndexsInfo.getInstance().loadIndexInfoFromDB();
        System.out.println(Env.homePath);
        String out = args[2];
        Category c = new Category(args[1]);
        if(args.length == 5) {
            c.content_name = args[4];
        }
        c.match(out, Integer.parseInt(args[3]));
    }
}

class Record {
    public String code;
    public String keywords;

    public Record(String code, String keywords) {
        this.code = code;
        this.keywords = keywords;
    }

    public String toString() {
        return "code:" + this.code + " keywords:" + keywords;
    }
}
