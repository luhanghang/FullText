package com.cis.fulltext;

import com.cis.utils.DB;
import com.cis.utils.Utils;
import com.cis.utils.Xml;
import com.fb.common.util.CommonUtil;
import com.fb.common.util.Env;
import com.fb.db.AllIndexsInfo;
import com.fb.db.FieldInfo;
import com.fb.db.IndexInfo;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.*;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Index {
    final public static String CONFIGFILE = "fulltext.xml";
    final public static int NEW = 0;
    final public static int UPDATE = 2;

    final public static int INDEX_ID = 0;
    final public static int INDEX_N_STORE = 1;
    final public static int ONLY_STORE = 2;
    final public static int ONLY_INDEX = 3;

    final public static int MAX_READ_COUNT = 2000;

    final public static Map<String, Integer> READCOUNT_MAP = new HashMap<String, Integer>();
    final public static Map<String, ParallelMultiSearcher> SEARCHER_MAP = new HashMap<String, ParallelMultiSearcher>();

    protected Xml config;
    protected String keyField;
    protected String name;
    protected String postfix = "";
    protected IndexInfo indexInf;
    protected String indexPath;
    protected String unionField = "combined";
    protected String[] unionFields;
    protected String[] outputFields;
    protected int maxReadCount = MAX_READ_COUNT;
    protected boolean unionEnabled = false;
    protected boolean indexFileExists = false;
    protected String contentField = "content";
    protected Node tfNode;
    protected String timeField = "jointime";
    protected boolean timeConvert = false;
    protected String orgTimeField;
    protected String idField;
    protected Map<String, String> fieldMap;
    protected Map<String, String> uFieldMap;
    protected String infType;
    protected String[] mSearch;

    protected Node uiNode;
    protected UnionIndex ui = null;

    protected Integer readCount = 0;
    protected ParallelMultiSearcher indexSearcher;
    protected int global_count = 0;

    static private Map<String, Index> indexList = new HashMap<String, Index>();

    static Category cate;
    static CategoryEng cateEng;

    String[] keywords = new String[4];

    public Index() {
        config = new Xml(CONFIGFILE);
    }

    public Index(String name) {
        this.name = name;
        init();
    }

    public Index(String name, String postfix) {
        this.name = name;
        this.postfix = postfix;
        init();
    }

    static public Index getInstance(String name) {
        Index instance = indexList.get(name);
        if (instance == null) {
            instance = new Index(name);
            indexList.put(name, instance);
        }
        return instance;
    }

    /*

    protected static MemCachedClient mcc = new MemCachedClient();

    static {
        String[] servers = {"127.0.0.1:6800"};

        Integer[] weights = {3};

        SockIOPool pool = SockIOPool.getInstance();

        pool.setServers(servers);
        pool.setWeights(weights);

        pool.setInitConn(5);
        pool.setMinConn(5);
        pool.setMaxConn(250);
        pool.setMaxIdle(1000 * 60 * 60 * 6);

        pool.setMaintSleep(30);

        pool.setNagle(false);
        pool.setSocketTO(3000);
        pool.setSocketConnectTO(0);

        pool.initialize();
    }

    */

    protected void init() {
        config = new Xml(CONFIGFILE);
        this.indexInf = AllIndexsInfo.getInstance().getIndexInfoFromDB(this.name);
        System.out.println(this.indexInf);
        this.indexPath = this.indexInf.indexPath + this.postfix;
        findKeyField();
        Node item = config.find("//Item[@id='" + this.name + "']");
        infType = this.name;
        Node typeNode = item.getAttributes().getNamedItem("type");
        if (typeNode != null) {
            this.infType = typeNode.getTextContent();
        }
        Node nIdField = item.getAttributes().getNamedItem("idField");
        if (nIdField != null)
            this.idField = nIdField.getTextContent();
        Node ufNode = Xml.find(item, "UnionFields");
        if (ufNode != null) {
            this.unionFields = ufNode.getTextContent().split(",");
        }
        tfNode = Xml.find(item, "TimeField");
        if (tfNode != null) {
            this.timeField = tfNode.getTextContent().trim();
            if (tfNode.getAttributes().getNamedItem("convert") != null) {
                this.timeConvert = true;
                this.orgTimeField = tfNode.getAttributes().getNamedItem("from").getTextContent();
            }
        }
        this.outputFields = Xml.find(item, "OutputFields").getTextContent().split(",");
        Node mNode = config.find("//MaxReadCount");
        if (mNode != null) {
            this.maxReadCount = Integer.parseInt(mNode.getTextContent().trim());
        }
        Node uNode = Xml.find(item, "UnionField");
        if (uNode != null) {
            this.unionField = uNode.getTextContent().trim();
        }

        Node msNode = Xml.find(item, "MultiSearch");
        if (msNode != null) {
            mSearch = msNode.getTextContent().trim().split(",");
        } else {
            mSearch = new String[1];
            mSearch[0] = this.name;
        }

        Node cNode = Xml.find(item, "ContentField");
        if (cNode != null) {
            this.contentField = cNode.getTextContent().trim();
        }
        this.uiNode = Xml.find(item, "UnionIndex");
        if (this.uiNode != null && Xml.find(this.uiNode, "@enable").getTextContent().equalsIgnoreCase("true")) {
            this.unionEnabled = true;
            this.ui = new UnionIndex(this.name);
            this.fieldMap = new HashMap<String, String>();
            this.uFieldMap = new HashMap<String, String>();
            NodeList fs = Xml.findAll(this.uiNode, "Map");
            for (int i = 0; i < fs.getLength(); i++) {
                this.fieldMap.put(Xml.getAttributeValue(fs.item(i), "this"), Xml.getAttributeValue(fs.item(i), "union"));
                this.uFieldMap.put(Xml.getAttributeValue(fs.item(i), "union"), Xml.getAttributeValue(fs.item(i), "this"));
            }
        }
        this.indexFileExists = IndexReader.indexExists(this.indexPath);

        try {
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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void bulk_create(String idFrom) throws Exception {
        if (this.keyField == null) {
            System.out.println("No key field, index can not be created");
            return;
        }

        log("Creating index....");

        long b = System.currentTimeMillis();
        String url = CommonUtil.getDBConnString(this.indexInf.dbType, this.indexInf.dbIP, this.indexInf.dbName);
        String driver = CommonUtil.getDbDriverStr(this.indexInf.dbType);
        Class.forName(driver).newInstance();

        IndexWriter indexWriter = this.getWriter();
        IndexWriter unionIndexWriter = this.unionEnabled ? this.ui.getWriter() : null;

        String lastRecord = idFrom;
        String lastRecordId = "";
        while (!lastRecord.equals("")) {
            StringBuffer sql = new StringBuffer("select * from ");
            if (this.timeConvert) {
                sql = new StringBuffer("select *, concat(if(left(" + this.orgTimeField +
                        ",1)='9','19','20'),left(" + this.orgTimeField + ",2),'-'," +
                        "mid(" + this.orgTimeField + ",3,2),'-',mid(" + this.orgTimeField + ", 5,2)) " +
                        "as " + this.timeField + " from ");
            }
            sql.append(this.indexInf.tableName);
            sql.append(" where ").append(this.idField).append(">").append(lastRecord);
            sql.append(" order by " + this.idField + " limit 50000");
            log("sql->" + sql.toString());
            Connection conn = DriverManager.getConnection(url, this.indexInf.userName, this.indexInf.passwd);
            Statement stmt = conn.createStatement();
            Connection _conn = DB.getConnection(1);
            Statement _stmt = _conn.createStatement();


            lastRecord = this.do_create(stmt.executeQuery(sql.toString()), indexWriter, unionIndexWriter, _stmt).lastRecord;
            if (!lastRecord.equals("")) {
                lastRecordId = lastRecord;
            }


            _stmt.close();
            _conn.close();
            stmt.close();
            conn.close();
        }

        indexWriter.close();
        if (unionIndexWriter != null) unionIndexWriter.close();


        long e = System.currentTimeMillis();
        log("Index created. " + (e - b) / 1000 + " seconds used. Last Record id is:" + lastRecordId);
    }

    public void bulk_create() throws Exception {
        this.bulk_create("0");
    }

    protected void findKeyField() {
        for (int i = 0; i < this.indexInf.vFieldInfo.size(); i++) {
            FieldInfo fieldInfo = (FieldInfo) this.indexInf.vFieldInfo.get(i);
            if (fieldInfo.indexType == Index.INDEX_ID) {
                this.keyField = fieldInfo.fieldName;
                return;
            }
        }
    }

    protected IndexReader initIndexReader() throws Exception {
        return IndexReader.open(this.indexPath);
    }

    protected ParallelMultiSearcher initIndexSearcher() throws Exception {
        IndexSearcher[] searchers = new IndexSearcher[this.mSearch.length];
        for (int i = 0; i < mSearch.length; i++) {
            searchers[i] = new IndexSearcher(new Index(mSearch[i]).indexPath);
        }
        return new ParallelMultiSearcher(searchers);
    }

    protected void removeOldDocument(String keyValue, IndexReader reader) throws Exception {
        if (this.indexFileExists) {
            Term term = new Term(this.keyField, keyValue);
            reader.deleteDocuments(term);
        }
    }

    protected IndexWriter getWriter() throws Exception {
        IndexWriter iw = new IndexWriter(this.indexPath, new StandardAnalyzer(), !this.indexFileExists);
        iw.setMaxFieldLength(100000);
        this.indexFileExists = true;
        return iw;
    }

    private void addUnionField(Document doc, String name, String value, Field.Store store, Field.Index index) {
        if (doc == null) return;
//        Node uField = Xml.find(this.uiNode, "Map[@this='" + name + "']/@union");
//        if (uField != null) {
//            doc.add(new Field(uField.getTextContent(), value, store, index));
//        }
        String ufield = this.fieldMap.get(name);
        if (ufield != null) {
            doc.add(new Field(ufield, value, store, index));
        }

    }

    private void update_content(Statement stmt, String seq, String content) throws Exception {
        String uri = this.name;
        if (this.name.equals("bidnotice") || this.name.equals("result")) {
            uri += "_recent";
        }
        String url = uri + "/getRecord.jsp?id=" + seq + "&recent=1";
        System.out.println(url);
        url = getMD5(url).toLowerCase();
        System.out.println(url);
        //mcc.delete(url);
        StringBuffer sql = new StringBuffer();
        sql.append("delete from contents.").append(this.infType).append(" where seq='").append(seq).append("'");
        stmt.execute(sql.toString());
        sql.delete(0, sql.length());
        sql.append("insert into contents.").append(this.infType).append(" values (null,'").append(seq).append("','").append(content.replaceAll("\\\\", "").replaceAll("'", "''")).append("')");

        System.out.println("content:" + seq);
        stmt.execute(sql.toString());
    }

    private void update_title(Statement stmt, String seq, String title, String infotype) throws Exception {
        title = title.replaceAll("'", "''").replaceAll("\\\\", "");
        StringBuffer sql = new StringBuffer();
        sql.append("delete from logs.titles where seq='").append(seq).append("' and infotype = '").append(infotype).append("'");
        stmt.execute(sql.toString());
        sql.delete(0, sql.length());
        sql.append("insert into logs.titles values (null,'").append(seq).append("',null,'").append(title).append("','").append(infotype).append("')");
        stmt.execute(sql.toString());
    }

    private CreateResult do_create(ResultSet rs, IndexWriter indexWriter, IndexWriter unionIndexWriter, Statement content_stmt) throws Exception {
        long b = System.currentTimeMillis();
        int count = 0;
        CreateResult createResult = new CreateResult();
        String lastRecord = "";
        while (rs.next()) {
            lastRecord = getRsString(rs, this.idField);
            Document doc = new Document();
            Document docUnion = this.unionEnabled ? new Document() : null;

            int selFieldNum = this.indexInf.vFieldInfo.size();
            for (int i = 0; i < selFieldNum; i++) {
                FieldInfo fieldInfo = (FieldInfo) this.indexInf.vFieldInfo.elementAt(i);
                String fieldValue = "";
                if (fieldInfo.indexType != Index.ONLY_INDEX) {
                    try {
                        fieldValue = getRsString(rs, fieldInfo.fieldName);
                    } catch (Exception e) {
                        continue;
                    }

                    if (fieldValue == null) fieldValue = "N";

                    if(fieldInfo.fieldName.equals("investscale")) {
                        int len = fieldValue.length();
                        StringBuffer ts = new StringBuffer();
                        for(int x = 0; x < 15 - len; x++) {
                            ts.append("0");
                        }
                        fieldValue = ts.toString() + fieldValue;
                    }

                    if (fieldInfo.fieldName.equals("vocation")) {
                        fieldValue = fieldValue.replaceAll(",", " A");
                        fieldValue = "A" + fieldValue;
                    }
                    if (fieldInfo.fieldName.equals("vipcate") || fieldInfo.fieldName.equals("fundsource")) {
                        fieldValue = fieldValue.replaceAll(",", " ");
                    }
                }

                Field.Store store = Field.Store.YES;
                Field.Index idx = Field.Index.TOKENIZED;
                switch (fieldInfo.indexType) {
                    case Index.INDEX_ID:
                        idx = Field.Index.UN_TOKENIZED;
                        break;
                    case Index.INDEX_N_STORE:
                        break;
                    case Index.ONLY_STORE:
                        idx = Field.Index.UN_TOKENIZED;
                        break;
                    case Index.ONLY_INDEX:
                        store = Field.Store.NO;
                        StringBuffer f = new StringBuffer();
                        for (String field : this.unionFields) {
                            f.append(getRsString(rs, field.trim())).append(",");
                        }
                        if (this.infType.equals("bidnotice") || this.infType.equals("result")) {
                            String fj = getRsString(rs, "fuj");
                            if (fj != null && !fj.equals("0")) {
                                Attachment attachment = new Attachment(this.name, getRsString(rs, this.keyField), fj);
                                String att = attachment.getContent();
                                System.out.println("fuj:" + fj + " length:" + att.length());
                                f.append(att);
                                createResult.hasAttachments = true;
                            }
                        }
                        fieldValue = f.toString().replaceAll(" ", "");
                        //fieldValue = Utils.numberToHan(fieldValue);
                        //System.out.println(fieldValue);
                        break;
                }
                if (fieldValue.trim().equals("") || fieldValue.trim().equals("-") || fieldValue.trim().equals("*"))
                    fieldValue = "N";

                doc.add(new Field(fieldInfo.fieldName, fieldValue, store, idx));
                this.addUnionField(docUnion, fieldInfo.fieldName, fieldValue, store, idx);
            }

            if (this.infType.equals("bidnotice")) {
                if (cate == null) {
                    cate = new Category("bidnotice");
                }

                if (cateEng == null) {
                    cateEng = new CategoryEng("bidnotice");
                }
                String c = cate.getCategory(getRsString(rs, this.contentField));
                String ce = cateEng.getCategory(getRsString(rs, this.contentField));
                doc.add(new Field("cate", c, Field.Store.YES, Field.Index.TOKENIZED));
                doc.add(new Field("cate_eng", ce, Field.Store.YES, Field.Index.TOKENIZED));
                log(getRsString(rs, this.keyField) + ":" + c + "/" + ce);
            }

            if (content_stmt != null) {
                try {
                    this.update_content(content_stmt, getRsString(rs, this.keyField), getRsString(rs, this.contentField));
                } catch (Exception e) {

                }
            }

            indexWriter.addDocument(doc);
            if (unionIndexWriter != null) {
                String type = this.infType;
                if (type.equals("project")) {
                    type = "project_dt";
                    String sid = getRsString(rs, "pro_sortid");
                    if (sid != null) {
                        sid = sid.trim();
                        if (sid.equals("02")) {
                            type = "project_nj";
                        } else if (sid.equals("03")) {
                            type = "project_zj";
                        }
                    }
                } else if (type.equals("bidnotice")) {
                    String free = getRsString(rs, "by3");
                    if (free != null && free.trim().equals("1")) {
                        type = "bidnotice_mf";
                    } else {
                        if (getRsString(rs, "bidtitle").indexOf(keywords[0]) >= 0 || getRsString(rs, "bidtitle").indexOf(keywords[1]) >= 0 || getRsString(rs, "bidtitle").indexOf(keywords[2]) >= 0) {
                            type = "bidnotice_bg";
                        } else if (getRsString(rs, "bidtitle").indexOf(keywords[3]) >= 0) {
                            type = "bidnotice_yg";
                        }
                    }
                }

                System.out.println("type:" + type);

                docUnion.add(new Field("infotype", type, Field.Store.YES, Field.Index.UN_TOKENIZED));
                System.out.println(type);
                unionIndexWriter.addDocument(docUnion);

                if (content_stmt != null) {
                    String title = getRsString(rs, this.uFieldMap.get("title"));
                    //title = new String(title.getBytes("ISO8859-1"),"GBK");
                    //title = new String(title.getBytes("ISO8859-1"),"UTF-8");
                    System.out.println(title);
                    this.update_title(content_stmt, getRsString(rs, this.keyField), title, type);
                }
            }
            log((++this.global_count) + "/" + (++count) + ":" + lastRecord);
        }
        long t = (System.currentTimeMillis() - b) / 1000;
        log(count + " records created, " + t / 60 + "minates used (" + t + "s)");

        rs.close();
        createResult.lastRecord = lastRecord;
        return createResult;
    }

    protected void prepareSearch() throws Exception {
        readCount = Index.READCOUNT_MAP.get(this.name + this.postfix);
        indexSearcher = Index.SEARCHER_MAP.get(this.name + this.postfix);

        if (readCount == null) {
            readCount = 0;
            Index.READCOUNT_MAP.put(this.name + this.postfix, readCount);
        }

        if (indexSearcher == null) {
            indexSearcher = this.initIndexSearcher();
            Index.SEARCHER_MAP.put(this.name + this.postfix, indexSearcher);
        }

        if (readCount >= this.maxReadCount) {
            try {
                indexSearcher.close();
            } catch (Exception e) {

            }
            indexSearcher = this.initIndexSearcher();
            Index.SEARCHER_MAP.put(this.name + this.postfix, indexSearcher);
//            Searchable srchbls[] = indexSearcher.getSearchables();
//            IndexSearcher is = (IndexSearcher)srchbls[0];
//            Directory d = is.getIndexReader().directory();
//            is.close();
//            srchbls[0] = new IndexSearcher(d);
            readCount = 0;
            Index.READCOUNT_MAP.put(this.name + this.postfix, readCount);
        }

        //Index.READCOUNT_MAP.put(this.name, ++readCount);
    }

    public void refresh() {
        Index.READCOUNT_MAP.put(this.name + this.postfix, this.maxReadCount);
    }

    public String getRecordById(String id) throws Exception {
        prepareSearch();
        StringBuffer out = new StringBuffer();
        Term t = new Term(this.keyField, id);
        TermQuery tq = new TermQuery(t);

        Hits hits = indexSearcher.search(tq);
        Document doc = hits.doc(hits.length() - 1);
        for (int i = 0; i < doc.getFields().size(); i++) {
            Field f = (Field) doc.getFields().get(i);
            if (f.name().equalsIgnoreCase(this.unionField) || f.name().equalsIgnoreCase(this.contentField)) continue;
            out.append(f.name()).append("|||").append(f.stringValue()).append("&&&");
        }
        out.append(this.contentField).append("|||").append(TxtContent.get(this.name, id));

        return out.toString();
    }

//    public String getRecordById(String id) throws Exception {
//        prepareSearch();
//        StringBuffer out = new StringBuffer();
//        Term t = new Term(this.keyField, id);
//        TermQuery tq = new TermQuery(t);
//
//        Hits hits = indexSearcher.search(tq);
//        Document doc = hits.doc(0);
//        for (int i = 0; i < doc.getFields().size(); i++) {
//            Field f = (Field) doc.getFields().get(i);
//            if (f.name().equals(this.unionField)) continue;
//            out.append(f.name()).append("|||").append(f.stringValue()).append("&&&");
//        }
//
//        return out.toString();
//    }

    public String getRecords(String condition, int page, int recordsPerPage) throws Exception {
        return getRecords(condition, page, recordsPerPage, null);
    }

    public int getMatchCount(String condition) throws Exception {
        prepareSearch();
        QueryParser parser = new QueryParser(this.keyField, new StandardAnalyzer());
        Query query = parser.parse(condition);
        Hits hits = indexSearcher.search(query);

        return hits.length();
    }

    public String getRecords(String condition, int page, int recordsPerPage, String sorts) throws Exception {
        if (this.tfNode != null) {
            condition += " AND " + this.timeField + ":[1990-01-01 TO 2050-12-31]";
        }
        prepareSearch();

        QueryParser parser = new QueryParser(this.keyField, new StandardAnalyzer());
        Query query = parser.parse("*:*");
        Hits hits = null;

        hits = indexSearcher.search(query);

        StringBuffer out = new StringBuffer();
        //out.append("total:").append(indexReader.numDocs()).append("\n\r");
        out.append("total:").append(hits.length()).append("\n\r");

        query = parser.parse(condition);

        Sort sort = new Sort();
        if (sorts == null) {
            SortField f_k = new SortField(this.keyField, SortField.STRING, true);
            if (this.tfNode != null) {
                SortField f_t = new SortField(this.timeField, SortField.STRING, true);
                sort.setSort(new SortField[]{f_t, f_k});
            } else {
                sort.setSort(f_k);
            }
        } else if(!sorts.equals("score")) {
            String[] ss = sorts.split(",");
            SortField[] sfs = new SortField[ss.length];
            for (int i = 0; i < ss.length; i++) {
                String[] t = ss[i].split(":");
                sfs[i] = new SortField(t[0], SortField.STRING, t[1].equals("0"));
            }
            sort.setSort(sfs);
        }

        if(sorts == null || !sorts.equals("score")) {
            hits = indexSearcher.search(query, sort);
        } else {
            hits = indexSearcher.search(query);
        }

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
                out.append("&&&hit_score|||" + hits.score(i));
                out.append("\n\r");
            }
        }

        return out.toString();
    }

    public void update() throws Exception {
        NodeList items = config.findAll("Item");
        for (int i = 0; i < items.getLength(); i++) {
            update(items.item(i).getAttributes().getNamedItem("id").getTextContent());
        }
    }

    public void update_vip() throws Exception {
        NodeList items = config.findAll("Item");
        for (int i = 0; i < items.getLength(); i++) {
            update_vip(items.item(i).getAttributes().getNamedItem("id").getTextContent());
        }
    }

    public void update(String item, String id) throws Exception {
        Index idx = new Index(item);
        idx.init();

        String url1 = CommonUtil.getDBConnString(idx.indexInf.dbType, idx.indexInf.dbIP, idx.indexInf.dbName);
        String driver1 = CommonUtil.getDbDriverStr(idx.indexInf.dbType);
        Class.forName(driver1).newInstance();
        Connection conn1 = DriverManager.getConnection(url1, idx.indexInf.userName, idx.indexInf.passwd);
        Statement stmt1 = conn1.createStatement();

        IndexReader reader = idx.initIndexReader();
        IndexReader uReader = idx.unionEnabled ? idx.ui.initIndexReader() : null;

        idx.removeOldDocument(id, reader);
        if (idx.unionEnabled) idx.ui.removeOldDocument(id, uReader);

        reader.close();
        if (uReader != null) {
            uReader.close();
        }

        IndexWriter indexWriter = idx.getWriter();
        IndexWriter unionIndexWriter = idx.unionEnabled ? idx.ui.getWriter() : null;

        Connection _conn = DB.getConnection(1);
        Statement _stmt = _conn.createStatement();
        //_stmt.execute("set names GBK");
        //stmt1.execute("set names GBK");
        StringBuffer sql1 = new StringBuffer("select * from ");
        if (idx.timeConvert) {
            sql1 = new StringBuffer("select *, concat(if(left(" + idx.orgTimeField +
                    ",1)='9','19','20'),left(" + idx.orgTimeField + ",2),'-'," +
                    "mid(" + idx.orgTimeField + ",3,2),'-',mid(" + idx.orgTimeField + ", 5,2)) " +
                    "as " + idx.timeField + " from ");
        }
        sql1.append(idx.indexInf.tableName).append(" where ").append(idx.keyField).append("='").append(id).append("'");

        idx.do_create(stmt1.executeQuery(sql1.toString()), indexWriter, unionIndexWriter, _stmt);


        _stmt.close();
        _conn.close();

        stmt1.close();
        conn1.close();

        //indexWriter.optimize();
        indexWriter.close();
        if (unionIndexWriter != null) {
            //unionIndexWriter.optimize();
            unionIndexWriter.close();
        }
    }

    public void update(String item) throws Exception {
        if (item.equals("union")) return;
        log("To update " + item);

        String sql = "select * from infoseek.infoaction where item='" + item + "' and (flag=0 or (flag = 11 and unix_timestamp(now()) - unix_timestamp(ctime) > 1200)) limit 500";
        Connection conn = DB.getConnection(1);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);

        if (rs == null || !rs.next()) {
            stmt.close();
            conn.close();
            return;
        }

        Index idx = new Index(item);
        idx.init();
        String url1 = CommonUtil.getDBConnString(idx.indexInf.dbType, idx.indexInf.dbIP, idx.indexInf.dbName);
        String driver1 = CommonUtil.getDbDriverStr(idx.indexInf.dbType);
        Class.forName(driver1).newInstance();
        Connection conn1 = DriverManager.getConnection(url1, idx.indexInf.userName, idx.indexInf.passwd);
        Statement stmt1 = conn1.createStatement();

        IndexReader reader = idx.initIndexReader();
        IndexReader uReader = idx.unionEnabled ? idx.ui.initIndexReader() : null;

        int count = 0;
        rs.beforeFirst();
        while (rs.next()) {
            String id = getRsString(rs, "uniseq");
            log(++count + ": remove " + id + " from index");
            idx.removeOldDocument(id, reader);
            if (idx.unionEnabled) idx.ui.removeOldDocument(id, uReader);
        }
        reader.close();
        if (uReader != null) {
            uReader.close();
        }

        IndexWriter indexWriter = idx.getWriter();
        IndexWriter unionIndexWriter = idx.unionEnabled ? idx.ui.getWriter() : null;

        String[] ids = new String[count];
        boolean[] fuj = new boolean[count];
        count = 0;
        rs.beforeFirst();

        Connection _conn = DB.getConnection(1);
        Statement _stmt = _conn.createStatement();


        while (rs.next()) {
            long b = System.currentTimeMillis();
            String id = getRsString(rs, "uniseq");
            ids[count] = getRsString(rs, "id");
            int opType = rs.getInt("action");

            if (opType == Index.UPDATE || opType == Index.NEW) {
                StringBuffer sql1 = new StringBuffer("select * from ");
                if (idx.timeConvert) {
                    sql1 = new StringBuffer("select *, concat(if(left(" + idx.orgTimeField +
                            ",1)='9','19','20'),left(" + idx.orgTimeField + ",2),'-'," +
                            "mid(" + idx.orgTimeField + ",3,2),'-',mid(" + idx.orgTimeField + ", 5,2)) " +
                            "as " + idx.timeField + " from ");
                }
                sql1.append(idx.indexInf.tableName).append(" where ").append(idx.keyField).append("='").append(id).append("'");

                fuj[count] = idx.do_create(stmt1.executeQuery(sql1.toString()), indexWriter, unionIndexWriter, _stmt).hasAttachments;
            }
            long e = System.currentTimeMillis();
            log(++count + " record updated: unseq=" + id + "." + (e - b) + " milliseconds used");

        }

        rs.close();

        for (int i = 0; i < ids.length; i++) {
            //sql = "insert into infoseek.infoaction_copy select * from infoseek.infoaction where id = " + ids[i];
            //stmt.execute(sql);
            if (fuj[i]) {
                sql = "update infoseek.infoaction set flag=flag+10 where id =" + ids[i];
            } else {
                sql = "update infoseek.infoaction set flag=1 where id =" + ids[i];
            }
            stmt.execute(sql);

        }


        _stmt.close();
        _conn.close();

        stmt1.close();
        conn1.close();

        stmt.close();
        conn.close();

        //indexWriter.optimize();
        indexWriter.close();
        if (unionIndexWriter != null) {
            //unionIndexWriter.optimize();
            unionIndexWriter.close();
        }
    }

    public void update_vip(String item) throws Exception {
        if (item.equals("union")) return;
        log("To update " + item);

        String sql = "select * from vipdb2005.infoaction where item='" + item + "'";
        String url = CommonUtil.getDBConnString("MySql", "localhost", "vipdb2005");
        String driver = CommonUtil.getDbDriverStr("MySql");
        Class.forName(driver).newInstance();
        Connection conn = DriverManager.getConnection(url, "root", "zzmzwj0328");
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);

        if (rs == null || !rs.next()) {
            stmt.close();
            conn.close();
            return;
        }

        Index idx = new Index(item);
        idx.init();
        String url1 = CommonUtil.getDBConnString(idx.indexInf.dbType, idx.indexInf.dbIP, idx.indexInf.dbName);
        String driver1 = CommonUtil.getDbDriverStr(idx.indexInf.dbType);
        Class.forName(driver1).newInstance();
        Connection conn1 = DriverManager.getConnection(url1, idx.indexInf.userName, idx.indexInf.passwd);
        Statement stmt1 = conn1.createStatement();

        IndexReader reader = idx.initIndexReader();

        int count = 0;
        Set<String> seqs = new HashSet<String>();
        rs.beforeFirst();
        while (rs.next()) {
            String id = getRsString(rs, "uniseq");
            log(++count + ": remove " + id + " from index");
            idx.removeOldDocument(id, reader);
            //if (idx.unionEnabled) idx.ui.removeOldDocument(id, uReader);
            //r_idx.removeOldDocument(id,r_reader);
            //r_uidx.removeOldDocument(id,r_uReader);
        }
        reader.close();
//        if (uReader != null) {
//            uReader.close();
//        }
//        r_reader.close();
//        r_uReader.close();

        IndexWriter indexWriter = idx.getWriter();
        //IndexWriter unionIndexWriter = idx.unionEnabled ? idx.ui.getWriter() : null;
        //IndexWriter r_iw = r_idx.getWriter();
        //IndexWriter r_uiw = r_uidx.getWriter();

        String[] ids = new String[count];
        count = 0;
        rs.beforeFirst();
        while (rs.next()) {
            long b = System.currentTimeMillis();

            String id = getRsString(rs, "uniseq");
            if (seqs.contains(id)) {
                log(id + " exists, pass.");
                continue;
            } else {
                seqs.add(id);
            }

            ids[count] = getRsString(rs, "id");
            int opType = rs.getInt("action");

            if (opType == Index.UPDATE || opType == Index.NEW) {
                StringBuffer sql1 = new StringBuffer("select * from ");
                if (idx.timeConvert) {
                    sql1 = new StringBuffer("select *, concat(if(left(" + idx.orgTimeField +
                            ",1)='9','19','20'),left(" + idx.orgTimeField + ",2),'-'," +
                            "mid(" + idx.orgTimeField + ",3,2),'-',mid(" + idx.orgTimeField + ", 5,2)) " +
                            "as " + idx.timeField + " from ");
                }
                sql1.append(idx.indexInf.tableName).append(" where ").append(idx.keyField).append("='").append(id).append("'");

                idx.do_create(stmt1.executeQuery(sql1.toString()), indexWriter, null, null);
            }
            long e = System.currentTimeMillis();
            log(++count + " record updated: unseq=" + id + "." + (e - b) + " milliseconds used");

        }

        rs.close();

        for (int i = 0; i < ids.length; i++) {
            if (ids[i] == null) break;
            sql = "delete from vipdb2005.infoaction where id =" + ids[i];
            stmt.execute(sql);
        }

        stmt1.close();
        conn1.close();

        stmt.close();
        conn.close();

        //indexWriter.optimize();
        indexWriter.close();
//        if (unionIndexWriter != null) {
//            //unionIndexWriter.optimize();
//            unionIndexWriter.close();
//        }
//        r_iw.close();
//        r_uiw.close();

        //Index.READCOUNT_MAP.put(item, Index.MAX_READ_COUNT);
    }

    protected void log(String s) {
        System.out.println("[" + Utils.now() + "] " + s);
    }

//    public String test(String item, String id, String recent) {
//        String uri = item;
//        String recent_tail = "";
//        if (recent != null) {
//            recent_tail = "&recent=1";
//            if (item.equals("bidnotice") || item.equals("result")) {
//                uri += "_recent";
//            }
//        }
//        String url = uri + "/getRecord.jsp?id=" + id + recent_tail;
//        try {
//            url = getMD5(url).toLowerCase();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return (String) mcc.get(url);
//    }

    public String getMD5(String sourceStr) {
        String resultStr = "";
        try {
            byte[] temp = sourceStr.getBytes();
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            md5.update(temp);
            // resultStr = new String(md5.digest());
            byte[] b = md5.digest();
            for (int i = 0; i < b.length; i++) {
                char[] digit = {'0', '1', '2', '3', '4', '5', '6', '7', '8',
                        '9', 'A', 'B', 'C', 'D', 'E', 'F'};
                char[] ob = new char[2];
                ob[0] = digit[(b[i] >>> 4) & 0X0F];
                ob[1] = digit[b[i] & 0X0F];
                resultStr += new String(ob);
            }
            return resultStr;
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        }
    }

    private String getRsString(ResultSet rs, String name) throws Exception {
        String value = rs.getString(name);
        if (value == null) return "";
        return value.replaceAll("'", "''");
//        try {
//            String value = rs.getString(name);
//            return new String(value.getBytes("ISO8859-1"),"GBK");
//        }
//        catch (Exception e) {
//            return null;
//        }
    }

    public String test(String id) throws Exception {
        String url = CommonUtil.getDBConnString(this.indexInf.dbType, this.indexInf.dbIP, this.indexInf.dbName);
        String driver = CommonUtil.getDbDriverStr(this.indexInf.dbType);
        Class.forName(driver).newInstance();
        Connection conn = DriverManager.getConnection(url, this.indexInf.userName, this.indexInf.passwd);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select title from " + this.indexInf.tableName + " where id = " + id);
        String value = null;
        while (rs.next()) {
            value = rs.getString("title");
        }
        rs.close();
        stmt.close();
        conn.close();
        return value;
    }

    public static void main(String[] args) throws Exception {
        Env.homePath = args[0];
        AllIndexsInfo.getInstance().loadIndexInfoFromDB();
        System.out.println(Env.homePath);
        if (args.length == 2) {
            Index idx = new Index(args[1]);
            idx.bulk_create();
        } else if (args.length == 3) {
            Index idx = new Index(args[1]);
            idx.bulk_create(args[2]);
        }
    }
}
