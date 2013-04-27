package com.cis.fulltext;

import com.fb.common.util.CommonUtil;
import com.fb.common.util.Env;
import com.fb.db.AllIndexsInfo;
import com.fb.db.FieldInfo;
import com.fb.db.IndexInfo;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class BuildMembersMemory {
    public static void main(String[] args) throws Exception {
        System.out.println("members memory index creating!");
        Env.homePath = args[0];

        Directory ramDir = new RAMDirectory();
        IndexInfo indexInfo = AllIndexsInfo.getInstance().getIndexInfoFromDB(args[1]);
        System.out.println(indexInfo);

        if (indexInfo == null) return;

        synchronized (indexInfo) {
            IndexWriter indexWriter = new IndexWriter(ramDir, new StandardAnalyzer(), true);
            indexWriter.setMaxFieldLength(100000);
            CommonUtil.getDBConnString(indexInfo.dbType, indexInfo.dbIP, indexInfo.dbName);
            String driver = CommonUtil.getDbDriverStr(indexInfo.dbType);
            Class.forName(driver).newInstance();
            Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/sms?useUnicode=true&characterEncoding=gbk&zeroDateTimeBehavior=convertToNull", "root", "zzwl0518");
            Statement stmt = conn.createStatement();
            Statement stmt1 = conn.createStatement();

            System.out.println("<br>begin to connect the database!<br>");


            String strSQL = "select count(*) from members ";
            ResultSet rs = stmt.executeQuery(strSQL);

            int nRowCount = 0;
            if (rs.next())
                nRowCount = rs.getInt(1);
            rs.close();

            int nPageSize = 500;
            int nPageCount = (nRowCount + nPageSize - 1) / nPageSize;
            int enterprise_id = 0;

            for (int j = 0; j < nPageCount; j++) {
                int offset = 0;
                int nBeginPos = j * nPageSize + 1;
                strSQL = "select * from members  limit " + nBeginPos + "," + nPageSize;
                rs = stmt.executeQuery(strSQL);
                System.out.println("<br>-----------------<br>");
                while (rs.next()) {
                    System.out.println((nBeginPos + (offset++)) + "/" + nRowCount);
                    Document doc1 = new Document();
                    enterprise_id = rs.getInt("enterprise_id");
                    String enstr = "select * from enterprises where id=" + enterprise_id;
                    ResultSet rs1 = stmt1.executeQuery(enstr);
                    int selFieldNum = indexInfo.vFieldInfo.size();

                    for (int i = 0; i < selFieldNum; i++) {
                        FieldInfo fieldInfo = (FieldInfo) indexInfo.vFieldInfo.elementAt(i);
                        String fieldValue = "";
                        if (fieldInfo.fieldName == "sale_id") {
                            fieldValue = Integer.toHexString(rs1.getInt("sale_id"));
                        }
                        if (fieldInfo.fieldName == "is_sale_submit") {
                            fieldValue = Integer.toHexString(rs1.getInt("is_sale_submit"));
                        }
                        if (fieldInfo.fieldName == "visit_records_time") {
                            fieldValue = rs1.getString("visit_records_time");
                        }
                        if (fieldInfo.fieldName == "visit_records_id") {
                            fieldValue = Integer.toHexString(rs1.getInt("visit_records_id"));
                        }
                        if (fieldInfo.fieldName == "member_id") {
                            if (rs1.getInt("member_id") == rs.getInt("id")) {
                                fieldValue = "66";
                            } else {
                                fieldValue = "99";
                            }

                        }


                        if (fieldInfo.fieldName == "id" || fieldInfo.fieldName == "source" || fieldInfo.fieldName == "infoid" || fieldInfo.fieldName == "enterprise_id" || fieldInfo.fieldName == "company_category_id" || fieldInfo.fieldName == "state") {
                            fieldValue = Integer.toHexString(rs.getInt(fieldInfo.fieldName));
                        }

                        if (fieldInfo.fieldName == "reg_name" || fieldInfo.fieldName == "full_name" || fieldInfo.fieldName == "username" || fieldInfo.fieldName == "contact" || fieldInfo.fieldName == "mobile" || fieldInfo.fieldName == "landline" || fieldInfo.fieldName == "fax" || fieldInfo.fieldName == "email" || fieldInfo.fieldName == "enter_time" || fieldInfo.fieldName == "reg_time" || fieldInfo.fieldName == "keywords" || fieldInfo.fieldName == "focus_proj" || fieldInfo.fieldName == "address" || fieldInfo.fieldName == "postcode" || fieldInfo.fieldName == "visit_time" || fieldInfo.fieldName == "next_visit" || fieldInfo.fieldName == "email_note" || fieldInfo.fieldName == "info_mail" || fieldInfo.fieldName == "member_category_id") {
                            fieldValue = rs.getString(fieldInfo.fieldName);
                        }


                        switch (fieldInfo.indexType) {
                            case 0:
                                doc1.add(new Field(fieldInfo.fieldName, fieldValue, Field.Store.YES, Field.Index.UN_TOKENIZED));
                                break;
                            case 1:
                                doc1.add(new Field(fieldInfo.fieldName, fieldValue, Field.Store.YES, Field.Index.TOKENIZED));
                                break;
                            case 2:
                                doc1.add(new Field(fieldInfo.fieldName, fieldValue, Field.Store.YES, Field.Index.UN_TOKENIZED));
                                break;
                            case 3:
                                fieldValue = rs.getString("reg_name") + rs.getString("full_name") + rs.getString("username") + rs.getString("contact");
                                fieldValue = fieldValue + rs.getString("mobile") + rs.getString("landline") + rs.getString("fax");
                                fieldValue = fieldValue + rs.getString("email") + rs.getString("address") + rs.getString("postcode");
                                fieldValue = fieldValue + rs.getString("note") + rs.getString("email_note") + rs.getString("info_mail");
                                doc1.add(new Field(fieldInfo.fieldName, fieldValue, Field.Store.NO, Field.Index.TOKENIZED));
                                break;
                        }
                    }

                    indexWriter.addDocument(doc1);
                    rs1.close();
                }

                rs.close();
            }
            indexWriter.close();
            stmt1.close();
            stmt.close();
            conn.close();
        }
    }
}
