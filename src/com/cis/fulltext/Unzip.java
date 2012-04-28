package com.cis.fulltext;

import org.apache.tools.zip.ZipEntry;
import org.apache.tools.zip.ZipFile;
import org.apache.tools.zip.ZipOutputStream;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;

/**
 * Created by IntelliJ IDEA.
 * User: luhang
 * Date: Nov 8, 2010
 * Time: 1:00:39 PM
 * To change this template use File | Settings | File Templates.
 */
public class Unzip {
    public static void unzip(String fileName) throws Exception{
        File file = new File("/tmp/attachments/" + fileName);
        //System.out.println("file:" + file.getAbsolutePath() + ":" + file.mkdir());
        file.mkdir();

        InputStream inputStream;
        FileOutputStream fileOut;
        int readedBytes;
        byte[] buf = new byte[512];

        ZipFile zf = new ZipFile("/mnt/attachments/" + fileName);

        for (Enumeration entries = zf.getEntries(); entries.hasMoreElements(); ) {
            ZipEntry entry = (ZipEntry) entries.nextElement();
            file = new File("/tmp/attachments/" + fileName + "/" + entry.getName());

            if (entry.isDirectory()) {
                file.mkdirs();
            } else {
                //File parent = file.getParentFile();
                //if (!parent.exists()) {
                //parent.mkdirs();
                //}

                inputStream = zf.getInputStream(entry);

                fileOut = new FileOutputStream(file);
                while ((readedBytes = inputStream.read(buf)) > 0) {
                    fileOut.write(buf, 0, readedBytes);
                }
                fileOut.close();

                inputStream.close();
            }
        }
        zf.close();
    }
}
