package com.fb.common.util;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Vector;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import com.fb.db.AllIndexsInfo;
import com.fb.db.DBInfoManager;
import com.fb.db.FieldInfo;
import com.fb.db.UnionFieldInfo;
import com.fb.db.IndexInfo;
import com.fb.db.UnionIndexInfo;
//import com.fb.struts.form.AddIndexForm;

public final class CommonUtil {

    /*
     * 获取数据库连接字符串
     */
    public static String getDBConnString(String dbType, String dbIP,
                                         String dbName) throws Exception {

        if (dbType.length() == 0 || dbIP.length() == 0 || dbName.length() == 0) {
            throw new Exception("数据库信息不正确，有信息为空!");
        }

        String connString = null;

        if (dbType.compareToIgnoreCase("mysql") == 0) {
            connString = "jdbc:mysql://" + dbIP + "/" + dbName
                    + "?useUnicode=true&characterEncoding=gbk";
        } else if (dbType.compareToIgnoreCase("sql server") == 0) {
            connString = "jdbc:microsoft:sqlserver://" + dbIP
                    + ":1433;DatabaseName=" + dbName;
        } else
            throw new Exception("数据库类型不正确!");

        return connString;
    }

    /*
     * 获取数据库驱动字符串
     */
    public static String getDbDriverStr(String dbType) throws Exception {

        if (dbType.length() == 0)
            throw new Exception("数据库类型为空!");

        String driverStr = null;

        if (dbType.compareToIgnoreCase("mysql") == 0) {
            driverStr = "com.mysql.jdbc.Driver";
        } else if (dbType.compareToIgnoreCase("sql server") == 0) {
            driverStr = "com.microsoft.jdbc.sqlserver.SQLServerDriver";
        } else
            throw new Exception("数据库类型不正确!");

        return driverStr;
    }

//	public static boolean writeIndexInfo2DB(AddIndexForm form,
//			String indexPath, String fieldInfo) {
//
//		try {
//			DBInfoManager dbManager = DBInfoManager.getInstance();
//			String url = CommonUtil.getDBConnString(dbManager.getDbType(),
//					dbManager.getDbIP(), dbManager.getDbName());
//
//			String driver = CommonUtil.getDbDriverStr(dbManager.getDbType());
//
//			Class.forName(driver).newInstance();
//			Connection conn = DriverManager.getConnection(url, dbManager
//					.getDbUser(), dbManager.getDbPasswd());
//
//			Statement stmt = conn.createStatement();
//			String indexName = new String(form.getIndexName()
//					.getBytes("8859_1"));
//
//			String strSQL = "insert into indexInfo(indexName,dbType,dbIP,dbName,passwd,userName,tableName,indexPath,fieldInfo) values(";
//			String strValue = "'" + indexName + "','" + form.getDbType()
//					+ "','" + form.getDbIP() + "','" + form.getDbName() + "','"
//					+ form.getPasswd() + "','" + form.getUserName() + "','"
//					+ form.getTableName() + "','" + indexPath + "','"
//					+ fieldInfo + "'";
//
//			strSQL = strSQL + strValue + ")";
//			stmt.execute(strSQL);
//
//			stmt.close();
//			conn.close();
//
//			// 重新加载所有的索引信息
//			AllIndexsInfo.getInstance().loadIndexInfoFromDB();
//
//			return true;
//		} catch (Exception e) {
//			return false;
//		}
//	}

    public static boolean writeIndexInfo2DB(UnionIndexInfo indexInfo) {

        try {
            DBInfoManager dbManager = DBInfoManager.getInstance();
            String url = CommonUtil.getDBConnString(dbManager.getDbType(),
                    dbManager.getDbIP(), dbManager.getDbName());

            String driver = CommonUtil.getDbDriverStr(dbManager.getDbType());

            Class.forName(driver).newInstance();
            Connection conn = DriverManager.getConnection(url, dbManager
                    .getDbUser(), dbManager.getDbPasswd());

            Statement stmt = conn.createStatement();

            String strSQL = "insert into unionIndexInfo(indexName,indexPath,fieldInfo,subIndexInfo) values(";
            String strValue = "'" + indexInfo.indexName + "','" + indexInfo.indexPath
                    + "','" + indexInfo.fieldInfo2String() + "','" + indexInfo.indexInfo2String() + "'";

            strSQL = strSQL + strValue + ")";
            stmt.execute(strSQL);

            stmt.close();
            conn.close();

            // 重新加载所有的索引信息
            AllIndexsInfo.getInstance().loadUnionIndexInfoFromDB();

            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static boolean getIndexInfoFromDB(Vector vIndexInfo)
            throws Exception {

        DBInfoManager dbManager = DBInfoManager.getInstance();
        String url = CommonUtil.getDBConnString(dbManager.getDbType(),
                dbManager.getDbIP(), dbManager.getDbName());

        String driver = CommonUtil.getDbDriverStr(dbManager.getDbType());

        Class.forName(driver).newInstance();
        Connection conn = DriverManager.getConnection(url, dbManager
                .getDbUser(), dbManager.getDbPasswd());

        Statement stmt = conn.createStatement();

        ResultSet rs = stmt.executeQuery("select * from indexInfo where tag=2010");

        while (rs.next()) {
            IndexInfo indexInfo = new IndexInfo();
            indexInfo.indexID = Integer.parseInt(rs.getString("id"));
            indexInfo.indexName = rs.getString("indexName");
            indexInfo.dbType = rs.getString("dbType");
            indexInfo.dbIP = rs.getString("dbIP");
            indexInfo.dbName = rs.getString("dbName");
            indexInfo.passwd = rs.getString("passwd");
            indexInfo.userName = rs.getString("userName");

            indexInfo.tableName = rs.getString("tableName");
            indexInfo.indexPath = rs.getString("indexPath");
            indexInfo.setFieldInfo(rs.getString("fieldInfo"));
            indexInfo.indexStatus = Integer.parseInt(rs
                    .getString("indexStatus"));

            vIndexInfo.add(indexInfo);
            System.out.println(indexInfo + " added");
        }

        stmt.close();
        rs.close();
        conn.close();

        return true;
    }

    public static boolean getUnionIndexInfoFromDB(Vector vIndexInfo)
            throws Exception {

        DBInfoManager dbManager = DBInfoManager.getInstance();
        String url = CommonUtil.getDBConnString(dbManager.getDbType(),
                dbManager.getDbIP(), dbManager.getDbName());

        String driver = CommonUtil.getDbDriverStr(dbManager.getDbType());

        Class.forName(driver).newInstance();
        Connection conn = DriverManager.getConnection(url, dbManager
                .getDbUser(), dbManager.getDbPasswd());

        Statement stmt = conn.createStatement();

        ResultSet rs = stmt.executeQuery("select * from unionIndexInfo");

        while (rs.next()) {
            UnionIndexInfo indexInfo = new UnionIndexInfo();
            indexInfo.indexID = Integer.parseInt(rs.getString("id"));
            indexInfo.indexName = rs.getString("indexName");
            indexInfo.indexPath = rs.getString("indexPath");
            indexInfo.setFieldInfo(rs.getString("fieldInfo"));
            indexInfo.setSubIndexInfo(rs.getString("subIndexInfo"));
            indexInfo.indexStatus = Integer.parseInt(rs.getString("indexStatus"));
            vIndexInfo.add(indexInfo);
        }

        stmt.close();
        rs.close();
        conn.close();

        return true;
    }

    public static boolean deleteIndexInfoFromDB(int nIndexID,int type) throws Exception {

        try {
            DBInfoManager dbManager = DBInfoManager.getInstance();
            String url = CommonUtil.getDBConnString(dbManager.getDbType(),
                    dbManager.getDbIP(), dbManager.getDbName());

            String driver = CommonUtil.getDbDriverStr(dbManager.getDbType());

            Class.forName(driver).newInstance();
            Connection conn = DriverManager.getConnection(url, dbManager
                    .getDbUser(), dbManager.getDbPasswd());

            Statement stmt = conn.createStatement();

            String strSQL = null;

            if(type == 0)
                strSQL = "delete from indexInfo where id = " + nIndexID;
            else if(type == 1)
                strSQL = "delete from unionIndexInfo where id = " + nIndexID;

            stmt.execute(strSQL);
            stmt.close();
            conn.close();

            // 重新加载所有的索引信息
            if(type == 0)
                AllIndexsInfo.getInstance().loadIndexInfoFromDB();
            else if(type == 1)
                AllIndexsInfo.getInstance().loadUnionIndexInfoFromDB();

            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static void reBuildSingleIndex(int nIndexID) throws Exception {
        IndexInfo indexInfo = AllIndexsInfo.getInstance().getIndexInfoFromDB(
                nIndexID);

        if (indexInfo == null)
            return;
        synchronized (indexInfo){
            IndexWriter indexWriter = new IndexWriter(indexInfo.indexPath,
                    new StandardAnalyzer(), true);

            String url = getDBConnString(indexInfo.dbType, indexInfo.dbIP,
                    indexInfo.dbName);

            String driver = getDbDriverStr(indexInfo.dbType);

            Class.forName(driver).newInstance();
            Connection conn = DriverManager.getConnection(url, indexInfo.userName,
                    indexInfo.passwd);

            Statement stmt = conn.createStatement();

            String strSQL = "select count(*) from " + indexInfo.tableName;
            ResultSet rs = stmt.executeQuery(strSQL); // 执行SQL语句并取得结果集

            int nRowCount = 0;
            if (rs.next())
                nRowCount = rs.getInt(1); // 获取总的数据记录行数
            rs.close(); // 关闭结果集

            int nPageSize = 50000;
            //int nPageSize = 5000;
            int nPageCount = (nRowCount + nPageSize - 1) / nPageSize;

            for (int j = 0; j < nPageCount; j++) {

                int nBeginPos = j * nPageSize + 1;

                strSQL = "select * from " + indexInfo.tableName + " limit " + nBeginPos + "," + nPageSize;
                rs = stmt.executeQuery(strSQL);

                while (rs.next()) {
                    Document doc1 = new Document();

                    int selFieldNum = indexInfo.vFieldInfo.size();

                    for (int i = 0; i < selFieldNum; i++) {
                        FieldInfo fieldInfo = (FieldInfo) indexInfo.vFieldInfo
                                .elementAt(i);
                        String fieldValue = rs.getString(fieldInfo.fieldName);

                        switch (fieldInfo.indexType) {
                            case 0: // 唯一标识
                                doc1.add(new Field(fieldInfo.fieldName, fieldValue,
                                        Field.Store.YES, Field.Index.UN_TOKENIZED));
                                break;
                            case 1: // 进行索引,且保存
                                doc1.add(new Field(fieldInfo.fieldName, fieldValue,
                                        Field.Store.YES, Field.Index.TOKENIZED));
                                break;
                            case 2: // 不进行索引,保存
                                doc1.add(new Field(fieldInfo.fieldName, fieldValue,
                                        Field.Store.YES, Field.Index.UN_TOKENIZED));
                                break;
                            case 3: // 进行索引,但不保存
                                doc1.add(new Field(fieldInfo.fieldName, fieldValue,
                                        Field.Store.NO, Field.Index.TOKENIZED));
                                break;
                        }
                    }

                    indexWriter.addDocument(doc1);
                }

                rs.close();
            }
            indexWriter.close();

            stmt.close();
            conn.close();

            // 更新索引状态
            DBInfoManager dbManager = DBInfoManager.getInstance();
            url = CommonUtil.getDBConnString(dbManager.getDbType(), dbManager
                    .getDbIP(), dbManager.getDbName());

            driver = CommonUtil.getDbDriverStr(dbManager.getDbType());

            conn = DriverManager.getConnection(url, dbManager.getDbUser(),
                    dbManager.getDbPasswd());

            stmt = conn.createStatement();

            strSQL = "update indexInfo set indexStatus = 1 where id = " + nIndexID;

            stmt.execute(strSQL);

            conn.close();
        }
    }

    //重建整体索引
    public static void reUnionBuildIndex(int nIndexID) throws Exception {

        UnionIndexInfo unionIndexInfo = AllIndexsInfo.getInstance().getUnionIndexInfoFromDB(nIndexID);
        if(unionIndexInfo == null)
            return;

        synchronized (unionIndexInfo){
            IndexWriter indexWriter = new IndexWriter(unionIndexInfo.indexPath,
                    new StandardAnalyzer(), true);

            for(int i=0; i<unionIndexInfo.vSubIndexInfo.size(); i++){
                IndexInfo indexInfo = (IndexInfo)unionIndexInfo.vSubIndexInfo.get(i);

                addUnionIndex(indexWriter,indexInfo,unionIndexInfo.vFieldInfo);
            }
            indexWriter.close();

            // 更新索引状态
            DBInfoManager dbManager = DBInfoManager.getInstance();
            String url = CommonUtil.getDBConnString(dbManager.getDbType(), dbManager
                    .getDbIP(), dbManager.getDbName());

            String driver = CommonUtil.getDbDriverStr(dbManager.getDbType());
            Class.forName(driver).newInstance();
            Connection conn = DriverManager.getConnection(url, dbManager.getDbUser(),
                    dbManager.getDbPasswd());

            Statement stmt = conn.createStatement();
            String strSQL = "update unionIndexInfo set indexStatus = 1 where id = " + nIndexID;
            stmt.execute(strSQL);
            conn.close();
        }
    }

    //	重建整体索引
    private static void addUnionIndex(IndexWriter indexWriter,IndexInfo indexInfo,Vector vFieldInfo) throws Exception {

        String url = getDBConnString(indexInfo.dbType, indexInfo.dbIP,
                indexInfo.dbName);

        String driver = getDbDriverStr(indexInfo.dbType);

        Class.forName(driver).newInstance();
        Connection conn = DriverManager.getConnection(url, indexInfo.userName,
                indexInfo.passwd);

        Statement stmt = conn.createStatement();

        String strSQL = "select count(*) from " + indexInfo.tableName;
        ResultSet rs = stmt.executeQuery(strSQL); // 执行SQL语句并取得结果集

        int nRowCount = 0;
        if (rs.next())
            nRowCount = rs.getInt(1); // 获取总的数据记录行数
        rs.close(); // 关闭结果集

        int nPageSize = 5000;
        int nPageCount =  (nRowCount + nPageSize - 1) / nPageSize;

        for (int j = 0; j < nPageCount; j++) {

            int nBeginPos = j * nPageSize + 1;

            strSQL = "select * from " + indexInfo.tableName + " limit " + nBeginPos + "," + nPageSize;
            rs = stmt.executeQuery(strSQL);

            while (rs.next()) {
                Document doc1 = new Document();

                //增加索引的相关信息
                doc1.add(new Field("indexName", indexInfo.indexName,Field.Store.YES, Field.Index.UN_TOKENIZED));

                int selFieldNum = vFieldInfo.size();
                for (int i = 0; i < selFieldNum; i++) {
                    UnionFieldInfo unionFieldInfo = (UnionFieldInfo) vFieldInfo.elementAt(i);
                    FieldInfo fieldInfo = unionFieldInfo.getIndexFieldInfo(indexInfo.indexName);

                    if(fieldInfo == null)
                        continue;

                    String fieldValue = rs.getString(fieldInfo.fieldName);

                    switch (fieldInfo.indexType) {
                        case 0: // 唯一标识
                            doc1.add(new Field(unionFieldInfo.fieldName, fieldValue,
                                    Field.Store.YES, Field.Index.UN_TOKENIZED));
                            break;
                        case 1: // 进行索引,且保存
                            doc1.add(new Field(unionFieldInfo.fieldName, fieldValue,
                                    Field.Store.YES, Field.Index.TOKENIZED));
                            break;
                        case 2: // 不进行索引,保存
                            doc1.add(new Field(unionFieldInfo.fieldName, fieldValue,
                                    Field.Store.YES, Field.Index.UN_TOKENIZED));
                            break;
                        case 3: // 进行索引,但不保存
                            doc1.add(new Field(unionFieldInfo.fieldName, fieldValue,
                                    Field.Store.NO, Field.Index.TOKENIZED));
                            break;
                    }
                }

                indexWriter.addDocument(doc1);
            }

            rs.close();
        }
        //	indexWriter.close();

        stmt.close();
        conn.close();
    }

    public static void reBuildIndex(int nIndexID,int type) throws Exception {
        if(type == 0)
            reBuildSingleIndex(nIndexID);
        else if(type == 1)
            reUnionBuildIndex(nIndexID);
    }

    public static void addIndex(IndexInfo indexInfo, String keyValue)
            throws Exception {

        synchronized (indexInfo){
//			 首先获得唯一标识字段
            String keyFieldName = null;

            for (int i = 0; i < indexInfo.vFieldInfo.size(); i++) {
                FieldInfo fieldInfo = (FieldInfo) indexInfo.vFieldInfo.get(i);
                if (fieldInfo.indexType == 0) {
                    keyFieldName = fieldInfo.fieldName;
                    break;
                }
            }

            if (keyFieldName == null)
                return;

            IndexWriter indexWriter = new IndexWriter(indexInfo.indexPath,
                    new StandardAnalyzer(), false);

            String url = getDBConnString(indexInfo.dbType, indexInfo.dbIP,
                    indexInfo.dbName);

            String driver = getDbDriverStr(indexInfo.dbType);

            Class.forName(driver).newInstance();
            Connection conn = DriverManager.getConnection(url, indexInfo.userName,
                    indexInfo.passwd);

            Statement stmt = conn.createStatement();

            String strSQL = "select * from " + indexInfo.tableName + " where "
                    + keyFieldName + "='" + keyValue + "'";
            ResultSet rs = stmt.executeQuery(strSQL);

            if (rs.next()) {
                Document doc1 = new Document();

                int selFieldNum = indexInfo.vFieldInfo.size();

                for (int i = 0; i < selFieldNum; i++) {
                    FieldInfo fieldInfo = (FieldInfo) indexInfo.vFieldInfo
                            .elementAt(i);
                    String fieldValue = rs.getString(fieldInfo.fieldName);

                    switch (fieldInfo.indexType) {
                        case 0: // 唯一标识
                            doc1.add(new Field(fieldInfo.fieldName, fieldValue,
                                    Field.Store.YES, Field.Index.UN_TOKENIZED));
                            break;
                        case 1: // 进行索引,且保存
                            doc1.add(new Field(fieldInfo.fieldName, fieldValue,
                                    Field.Store.YES, Field.Index.TOKENIZED));
                            break;
                        case 2: // 不进行索引,保存
                            doc1.add(new Field(fieldInfo.fieldName, fieldValue,
                                    Field.Store.YES, Field.Index.UN_TOKENIZED));
                            break;
                        case 3: // 进行索引,但不保存
                            doc1.add(new Field(fieldInfo.fieldName, fieldValue,
                                    Field.Store.NO, Field.Index.TOKENIZED));
                            break;
                    }
                }

                indexWriter.addDocument(doc1);
            }

            rs.close();

            indexWriter.close();

            stmt.close();
            conn.close();
        }
    }

    public static void deleteIndex(IndexInfo indexInfo, String keyValue)
            throws IOException {

        synchronized (indexInfo){
            // 首先获得唯一标识字段
            String keyFieldName = null;

            for (int i = 0; i < indexInfo.vFieldInfo.size(); i++) {
                FieldInfo fieldInfo = (FieldInfo) indexInfo.vFieldInfo.get(i);
                if (fieldInfo.indexType == 0) {
                    keyFieldName = fieldInfo.fieldName;
                    break;
                }
            }

            if (keyFieldName == null)
                return;

            Directory directory = FSDirectory.getDirectory(indexInfo.indexPath,
                    false);
            IndexReader reader = IndexReader.open(directory);

            Term term = new Term(keyFieldName, keyValue);
            reader.deleteDocuments(term);
            reader.close();
            directory.close();
        }

    }

    public static void updateIndex(IndexInfo indexInfo, String keyValue)
            throws Exception {

        deleteIndex(indexInfo, keyValue);

        addIndex(indexInfo, keyValue);
    }

    public static String markAllKeywordRed(String strSrc, Vector vKey) {
        for(int i=0; i<vKey.size(); i++){
            String strSrc2 = markRed(strSrc,(String)vKey.get(i));
            strSrc = strSrc2;
        }

        return strSrc;
    }

    public static String markRed(String strSrc, String strKey) {
        if(strSrc == null || strKey == null)
            return null;

        // 先利用空格将关键字分割
        Vector vKeys = new Vector();

        strKey = strKey + " ";
        int nPos = strKey.indexOf(' ');
        while (nPos != -1) {
            String str = strKey.substring(0, nPos);
            if (!strKey.equals(""))
                vKeys.add(str);
            strKey = strKey.substring(nPos + 1);
            nPos = strKey.indexOf(' ');
        }

        for (int i = 0; i < vKeys.size(); i++) {
            String str = (String) vKeys.get(i);
            String dest = "<font color=#FF0000>" +  str + "</font>";
            strSrc = strSrc.replaceAll(str, dest);
        }

        return strSrc;
    }

//	public static Vector getTableFieldName(AddIndexForm form) throws Exception {
//
//		String url = CommonUtil.getDBConnString(form.getDbType(), form
//				.getDbIP(), form.getDbName());
//
//		String driver = CommonUtil.getDbDriverStr(form.getDbType());
//
//		Class.forName(driver).newInstance();
//		Connection conn = DriverManager.getConnection(url, form.getUserName(),
//				form.getPasswd());
//
//		PreparedStatement pstmt = conn.prepareStatement("SELECT * FROM "
//				+ form.getTableName() + " where 1 = 2 ");
//
//		ResultSet rs = pstmt.executeQuery();
//
//		ResultSetMetaData rsmd = rs.getMetaData(); // 获取字段名
//
//		Vector vFieldName = new Vector();
//		if (rsmd != null) {
//			int count = rsmd.getColumnCount();
//			for (int i = 1; i <= count; i++) {
//				vFieldName.add(rsmd.getColumnName(i));
//			}
//		}
//
//		conn.close();
//
//		return vFieldName;
//	}
}
