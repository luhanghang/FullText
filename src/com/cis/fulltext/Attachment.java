package com.cis.fulltext;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hwpf.extractor.WordExtractor;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * Created by IntelliJ IDEA.
 * User: luhang
 * Date: Sep 3, 2010
 * Time: 11:08:40 AM
 * To change this template use File | Settings | File Templates.
 */
public class Attachment {
    private String item;
    private String seq;
    private String fuj;

    public Attachment(String item, String seq, String fileName) {
        this.item = item;
        this.seq = seq;
        this.fuj = fileName;
    }

    public String getExt(String fileName) {
//        int dot = fileName.lastIndexOf(".");
//        if (dot == -1) return "";
//        return fileName.substring(dot + 1, fileName.length()).toLowerCase();
        if (fileName.length() < 5) return "";
        return fileName.substring(fileName.length() - 3, fileName.length()).toLowerCase();
    }

    public String readWord(File file) {
        try {
            InputStream is = new FileInputStream(file);
            String content = readWord(is).toString();
            is.close();
            return content;
        } catch (Exception e) {
            logError("read word", e.getMessage());
        }
        return "";
    }

    public String readExcel(File file) {
        try {
            InputStream is = new FileInputStream(file);
            String content = readExcel(is).toString();
            is.close();
            return content;
        } catch (Exception e) {
            logError("read excel", e.getMessage());
        }
        return "";
    }

    public StringBuffer readWord(InputStream is) {
        StringBuffer text = new StringBuffer();
        try {
            WordExtractor ex = new WordExtractor(is);
            text.append(ex.getText());
            //System.out.println(new String(bodyText.getBytes("ISO8859_1"), "GBK"));
        } catch (Exception e) {
            logError("read word", e.getMessage());
        }
        return text;
    }

    public StringBuffer readExcel(InputStream is) {
        StringBuffer content = new StringBuffer();
        try {
            HSSFWorkbook workbook = new HSSFWorkbook(is);
            for (int numSheets = 0; numSheets < workbook.getNumberOfSheets(); numSheets++) {
                if (null != workbook.getSheetAt(numSheets)) {
                    HSSFSheet aSheet = workbook.getSheetAt(numSheets);
                    for (int rowNumOfSheet = 0; rowNumOfSheet <= aSheet.getLastRowNum(); rowNumOfSheet++) {
                        if (null != aSheet.getRow(rowNumOfSheet)) {
                            HSSFRow aRow = aSheet.getRow(rowNumOfSheet);
                            for (int cellNumOfRow = 0; cellNumOfRow <= aRow.getLastCellNum(); cellNumOfRow++) {
                                if (null != aRow.getCell(cellNumOfRow)) {
                                    HSSFCell aCell = aRow.getCell(cellNumOfRow);
                                    if (HSSFCell.CELL_TYPE_STRING == aCell.getCellType())
                                        content.append(aCell.getStringCellValue());
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            logError("read excel", e.getMessage());
        }
        return content;
    }

    public String readHtml(File file) {
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            StringBuffer content = new StringBuffer();
            int i = 0;
            String data = null;
            while ((data = br.readLine()) != null) {
                content.append(data);
            }
            br.close();
            return content.toString();
        } catch (Exception e) {
            logError("read html", e.getMessage());
            return "";
        }
    }

//    public String readZip(String fileName) {
//        try {
//            StringBuffer content = new StringBuffer();
//            int offset = 1;
//            while (true) {
//                InputStream is = new FileInputStream(fileName);
//                ZipInputStream zis = new ZipInputStream(is);
//                ZipEntry zipEntry = null;
//                for (int i = 0; i < offset; i++)
//                    zipEntry = zis.getNextEntry();
//
//                if (zipEntry == null) break;
//
//                String file = zipEntry.getName();
//                String ext = getExt(file);
//                System.out.println(offset + ":" + file);
//
//                if (ext.equals("doc")) {
//                    content.append(readWord(zis));
//                }
//                if (ext.equals("xls")) {
//                    content.append(readExcel(zis));
//                }
//                offset++;
//                is.close();
//            }
//            //return new String(content.toString().getBytes("ISO8859_1"),"GBK");
//            return content.toString();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return "";
//    }

    public String getFiles(File _file) {
        StringBuffer content = new StringBuffer();
        try {
            if (!_file.exists()) {
                logError("get file", "Directory " + _file.getAbsolutePath() + " does not exist");
                System.out.println("Directory " + _file.getAbsolutePath() + " does not exist");
                return "";
            }
            if (_file.isFile()) {
                System.out.println("file:" + _file.getName());
                if (getExt(_file.getName()).equals("xls")) {
                    return readExcel(_file.getAbsoluteFile());
                }
                if (getExt(_file.getName()).equals("doc")) {
                    return readWord(_file.getAbsoluteFile());
                }
                if (getExt(_file.getName()).endsWith("htm") || getExt(_file.getName()).endsWith("tml") || getExt(_file.getName()).endsWith("txt")) {
                    return readHtml(_file.getAbsoluteFile());
                }
                return "";
            }
            File[] files = _file.listFiles();

            for (int i = 0; i < files.length; i++) {
                content.append(getFiles(files[i]));
            }
        } catch (Exception e) {
            logError("get files", e.getMessage());
            return "";
        }
        return content.toString();
    }

//    public String getRecordById(String id) throws Exception {
//        IndexSearcher indexSearcher = new IndexSearcher("/tmp/index");
//        StringBuffer out = new StringBuffer();
//        Term t = new Term("key", id);
//        TermQuery tq = new TermQuery(t);
//
//        Hits hits = indexSearcher.search(tq);
//        Document doc = hits.doc(0);
//        for (int i = 0; i < doc.getFields().size(); i++) {
//            Field f = (Field) doc.getFields().get(i);
//            out.append(f.name()).append("|||").append(f.stringValue()).append("&&&");
//        }
//        indexSearcher.close();
//        return out.toString();
//    }

//    public String getRecords(String condition, int page, int recordsPerPage) throws Exception {
//        IndexSearcher indexSearcher = new IndexSearcher("/tmp/index");
//
//        QueryParser parser = new QueryParser("key", new StandardAnalyzer());
//        Query query = parser.parse("*:*");
//        Hits hits = null;
//
//        hits = indexSearcher.search(query);
//
//        StringBuffer out = new StringBuffer();
//        //out.append("total:").append(indexReader.numDocs()).append("\n\r");
//        out.append("total:").append(hits.length()).append("\n\r");
//
//        query = parser.parse(condition);
//        hits = indexSearcher.search(query);
//
//        int hitNum = hits.length();
//
//        out.append("hits:").append(hitNum).append("\n\r");
//
//        int nBeginIndex = (page - 1) * recordsPerPage;
//        int nEndIndex = page * recordsPerPage;
//
//        for (int i = nBeginIndex; i < hitNum && i < nEndIndex; i++) {
//            out.append(i + 1);
//            out.append("&&&").append(hits.doc(i).get("key")).append("&&&fj&&&").append(hits.doc(i).get("fj"));
//            out.append("\n\r");
//        }
//
//        return out.toString();
//    }

    public String getContent() {
        if (!fuj.startsWith("/")) fuj = "/" + fuj;
        if (!fuj.startsWith("/uploadfile")) fuj = "/uploadfile" + fuj;
        String ext = getExt(fuj);
        System.out.println(fuj + "//ext:" + ext);
        File file = new File("/mnt/attachments" + fuj);
        if (!file.exists()) {
            logError("find file", "/mnt/attachments" + fuj + " does not exist");
            System.out.println("File:/mnt/attachments" + fuj + " does not exist");
            return "";
        }
        Process proc;
        if (ext.equals("tml") || ext.equals("htm") || ext.equals("txt") || ext.equals("doc") || ext.equals("xls") || ext.equals("zip") || ext.equals("rar")) {
            if (ext.equals("zip")) {
//                proc = Runtime.getRuntime().exec("mkdir /tmp/attachments/" + fuj);
//                proc.waitFor();
//                proc = Runtime.getRuntime().exec("/usr/local/bin/unzip /mnt/attachments/" + fuj + " -d /tmp/attachments/" + fuj);
//                proc.waitFor();
                try {
                    Unzip.unzip(fuj);
                } catch (Exception e) {
                    logError("unzip", e.getMessage());
                }
                String content = getFiles(new File("/tmp/attachments" + fuj));
                //File file = new File("/tmp/attachments/" + fuj);
                try {
                    proc = Runtime.getRuntime().exec("rm -rf /tmp/attachments" + fuj);
                    proc.waitFor();
                    proc.destroy();
                } catch (Exception e) {
                    logError("rm -rf /tmp/attachments" + fuj, e.getMessage());
                }
                return content;
            }

            if (ext.equals("rar")) {
                try {
                    proc = Runtime.getRuntime().exec("mkdir /tmp/attachments" + fuj);
                    proc.waitFor();
                    System.out.println("unrar x -o+ -p- /mnt/attachments" + fuj + " /tmp/attachments" + fuj);
                } catch (Exception e) {
                    logError("unrar x -o+ -p- /mnt/attachments" + fuj + " /tmp/attachments" + fuj, e.getMessage());
                }
                try {
                    proc = Runtime.getRuntime().exec("unrar x -o+ -p- -y /mnt/attachments" + fuj + " /tmp/attachments" + fuj);
                    proc.waitFor();
                } catch (Exception e) {
                    logError("unrar x -o+ -p- -y /mnt/attachments" + fuj + " /tmp/attachments" + fuj, e.getMessage());
                }
                System.out.println("rar uncompressed");
                String content = getFiles(new File("/tmp/attachments" + fuj));
                try {
                    proc = Runtime.getRuntime().exec("rm -rf /tmp/attachments" + fuj);
                    proc.waitFor();
                    proc.destroy();
                } catch (Exception e) {

                }
                return content;
            }

            if (ext.equals("doc")) {
                return readWord(file);
            }

            if (ext.equals("xls")) {
                return readExcel(file);
            }

            if (ext.equals("tml") || ext.equals("htm") || ext.equals("txt")) {
                return readHtml(file);
            }
        }
        return "";
    }

    private void logError(String when, String error) {
        System.out.println("!!!! Error -> When " + when + ", error occured:" + error);
        StringBuffer sql = new StringBuffer();
        sql.append("insert into logs.attachment_error values (null,now(),'").append(this.item).append("','");
        sql.append(this.seq).append("','").append(this.fuj).append("','").append(when).append("','").append(error).append("')");
        String url = "jdbc:mysql://localhost/logs?useUnicode=true&characterEncoding=latin1";
        String driver = "com.mysql.jdbc.Driver";
        try {
            Class.forName(driver).newInstance();
            Connection conn = DriverManager.getConnection(url, "root", "zzwl0518");
            Statement stmt = conn.createStatement();
            stmt.execute(sql.toString());
            stmt.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void main(String[] args) throws Exception {
        String filename = args[0];
        System.out.println(args[0]);
        if (!filename.startsWith("/uploadfile")) {
            filename = "/uploadfile" + filename;
        }
        System.out.println(filename);
        //getContent(filename);
    }
}
