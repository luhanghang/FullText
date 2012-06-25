package com.cis.fulltext;

import com.cis.utils.DB;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.*;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.store.Directory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import com.cis.utils.Xml;
import com.fb.common.util.Env;
import com.fb.db.AllIndexsInfo;

import java.sql.Connection;
import java.sql.Statement;
import java.util.*;

public class UnionIndex extends Index {
    protected String type = null;
    protected static ParallelMultiSearcher searcher;
    public static List<String> types;

    final public static int U_MAX_READ_COUNT = 2000;
    private static UnionIndex instance;
    Set<Integer> not_reload = new HashSet<Integer>();

    public UnionIndex() {
        super("union");
        initTypes();
    }

    protected void initTypes() {
        if (types == null) {
            types = new ArrayList<String>();
            NodeList nl = config.findAll("//Item/UnionIndex[@enable='true']");
            for (int i = 0; i < nl.getLength(); i++) {
                Node node = nl.item(i).getParentNode();
                types.add(Xml.getAttributeValue(node, "id"));
                if (node.getAttributes().getNamedItem("type") != null) {
                    not_reload.add(i);
                }
            }
        }
    }

    static public UnionIndex getInstance() {
        if (instance == null) {
            instance = new UnionIndex();
        }
        return instance;
    }

    public UnionIndex(String type) {
        super("union");
        this.type = type;
        this.indexPath = this.indexInf.indexPath + "_" + this.type;
        this.indexFileExists = IndexReader.indexExists(this.indexPath);
    }

    protected ParallelMultiSearcher initMultiIndexSearcher() throws Exception {
        IndexSearcher[] searchers = new IndexSearcher[types.size()];
        for (int i = 0; i < types.size(); i++) {
            String type = types.get(i);
            searchers[i] = new IndexSearcher(this.indexInf.indexPath + "_" + type);
        }
        return new ParallelMultiSearcher(searchers);
    }

    protected void prepareSearch() throws Exception {
        readCount = Index.READCOUNT_MAP.get(this.name);

        if (readCount == null) {
            readCount = 0;
            Index.READCOUNT_MAP.put(this.name, readCount);
        }

        if (searcher == null) {
            searcher = this.initMultiIndexSearcher();
        }

        if (readCount >= U_MAX_READ_COUNT) {
            try {
                searcher.close();
            } catch (Exception e) {

            }
            searcher = this.initMultiIndexSearcher();
//            Searchable srchbls[] = searcher.getSearchables();
//            for (int i = 0; i < srchbls.length; i++) {
//                if (!not_reload.contains(i)) {
//                    IndexSearcher is = (IndexSearcher) srchbls[i];
//                    Directory d = is.getIndexReader().directory();
//                    is.close();
//                    srchbls[i] = new IndexSearcher(d);
//                }
//            }

            readCount = 0;
            Index.READCOUNT_MAP.put(this.name, readCount);
        }

        //Index.READCOUNT_MAP.put(this.name, ++readCount);
    }

    public void refresh() {
        Index.READCOUNT_MAP.put(this.name, U_MAX_READ_COUNT);
    }

    public void getAllRecords(int from) throws Exception {
        prepareSearch();

        QueryParser parser = new QueryParser(this.keyField, new StandardAnalyzer());
        Query query = parser.parse("*:*");

        Hits hits = searcher.search(query);

        int to = from + 10000;
        if (to >= hits.length()) {
            to = hits.length();
        }

        while (to < hits.length()) {
            Connection _conn = DB.getConnection(1);
            Statement _stmt = _conn.createStatement();
            for (int i = from; i < to; i++) {
                Document doc = hits.doc(i);
                String seq = doc.get("uniseq");
                String infotype = doc.get("infotype");
                String title = doc.get("title");
                StringBuffer sql = new StringBuffer();
                sql.append("delete from logs.titles where seq='").append(seq).append("' and infotype = '").append(infotype).append("'");
                _stmt.execute(sql.toString());
                sql.delete(0, sql.length());
                sql.append("insert into logs.titles values (null,'").append(seq).append("',null,'").append(title).append("','").append(infotype).append("')");
                try {
                    _stmt.execute(sql.toString());
                } catch (Exception e) {
                    e.printStackTrace();
                }
                System.out.println(i + ":" + sql);
            }
            from = to;
            to = from + 10000;
            if (to >= hits.length()) {
                to = hits.length();
            }
            _stmt.close();
            _conn.close();
            System.out.println("-------------------------------");
        }
        System.out.println("done");
    }

    public int getMatchCount(String condition) throws Exception {
        prepareSearch();
        QueryParser parser = new QueryParser(this.keyField, new StandardAnalyzer());
        Query query = parser.parse(condition);
        Hits hits = searcher.search(query);

        return hits.length();
    }

    public String getRecords(String condition, int page, int recordsPerPage) throws Exception {
        condition += " AND " + this.timeField + ":[1990-01-01 TO 2050-12-31]";
        prepareSearch();

        StringBuffer out = new StringBuffer();
        out.append("total:0\n\r");

        QueryParser parser = new QueryParser(this.keyField, new StandardAnalyzer());
        Query query = parser.parse(condition);

        Sort sort = new Sort();
        SortField f_t = new SortField(this.timeField, SortField.STRING, true);
        SortField f_k = new SortField(this.keyField, SortField.STRING, true);
        SortField f_infotype = new SortField("infotype", SortField.STRING, false);
        sort.setSort(new SortField[]{f_t, f_k, f_infotype});

        Hits hits = null;

        hits = searcher.search(query, sort);

        int hitNum = hits.length();

        out.append("hits:").append(hitNum).append("\n\r");

        int nBeginIndex = (page - 1) * recordsPerPage;
        int nEndIndex = (page + 1) * recordsPerPage;
        int seq = 0;
        for (int i = nBeginIndex; i < hitNum && i < nEndIndex && seq < recordsPerPage; i++) {
            String id = hits.doc(i).get(this.keyField);
            boolean unique = true;
            for (int j = i + 1; j < hitNum && j < nEndIndex; j++) {
                if (id.equals(hits.doc(j).get(this.keyField))) {
                    unique = false;
                    break;
                }
            }
            if (unique) {
                out.append(++seq);
                for (String field : this.outputFields) {
                    String value = hits.doc(i).get(field);
                    if (value == null) value = "";
                    out.append("&&&").append(field).append("|||").append(value.replaceAll("\n", " ").replaceAll("\r", ""));
                }
                out.append("\n\r");
            }
        }

//        int nEndIndex = page * recordsPerPage;
//
//        for (int i = nBeginIndex; i < hitNum && i < nEndIndex; i++) {
//            out.append(i + 1);
//            for (String field : this.outputFields) {
//                String fieldvalue = hits.doc(i).get(field);
//                if (fieldvalue == null) {
//                    fieldvalue = "N";
//                }
//                out.append("&&&").append(field).append("|||").append(fieldvalue.replaceAll("\n", " ").replaceAll("\r", ""));
//            }
//            out.append("\n\r");
//        }

        return out.toString();
    }

    protected IndexReader initIndexReader() throws Exception {
        return IndexReader.open(this.indexPath);
    }

    public static void main(String[] args) throws Exception {
        Env.homePath = args[0];
        AllIndexsInfo.getInstance().loadIndexInfoFromDB();
        UnionIndex u = new UnionIndex();
        System.out.println(u.not_reload);
        System.out.println(UnionIndex.types);
        System.out.println(u.getRecords("*:*", 1, 40));
    }
}
