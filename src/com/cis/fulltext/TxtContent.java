package com.cis.fulltext;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import com.cis.utils.DB;

public class TxtContent {

    public static String get(String indexName, String seq) throws Exception{
        String content = "";
        StringBuffer sql = new StringBuffer();
        sql.append("select content from contents.").append(indexName).append(" where seq='").append(seq).append("'");
        Connection conn = DB.getConnection(1);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql.toString());
        rs.beforeFirst();
        if(rs.next()) {
            content = rs.getString("content");
            rs.close();
        }
        stmt.close();
        conn.close();
        return content;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.out.println("Usage:TxtContent src_table,des_table,idField,seqField,lastRecord,db=0,content_field=content");
            return;
        }

        long b = System.currentTimeMillis();

        String src_table = args[0];
        String des_table = args[1];
        String idField = args[2];
        String seqField = args[3];
        String lastRecord = args[4];
        String content_field = "content";
        int db = 0;
        if(args.length > 5) {
            db = Integer.parseInt(args[5]);
        }

        if(args.length > 6) {
            content_field = args[6];
        }

        Connection conn = DB.getConnection(db);
        Statement stmt = conn.createStatement();

        Connection _conn = DB.getConnection(1);
        Statement _stmt = _conn.createStatement();

        int count = 0;
        StringBuffer sql = new StringBuffer();
        sql.append("select count(1) from ").append(src_table);
        ResultSet rs = stmt.executeQuery(sql.toString());
        rs.first();
        String ttl = rs.getString(1);
        rs.close();

        while (true) {
            sql.delete(0, sql.length());
            sql.append("select ").append(idField).append(",").append(seqField).append(",").append(content_field).append(" from ").append(src_table);
            sql.append(" where ").append(idField).append(">").append(lastRecord).append(" limit 10000");

            System.out.println(sql.toString());
            rs = stmt.executeQuery(sql.toString());

            if (rs == null || !rs.next()) {
                break;
            }
            rs.beforeFirst();
            while (rs.next()) {
                sql.delete(0, sql.length());
                sql.append("insert into ").append(des_table).append(" values (null,'").append(rs.getString(2)).append("','").append(rs.getString(3).replaceAll("'", "''")).append("')");
                try {
                    _stmt.execute(sql.toString());
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
                lastRecord = rs.getString(1);
                System.out.println(++count + ":" + ttl + " id=" + lastRecord);
            }
        }
        stmt.close();
        conn.close();

        _stmt.close();
        _conn.close();
        long e = System.currentTimeMillis();
        System.out.println("Finished. " + (e - b) / 1000 + " seconds used.");
        return;
    }
}
