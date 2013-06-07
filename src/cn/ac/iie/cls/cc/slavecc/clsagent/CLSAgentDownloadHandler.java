/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slavecc.clsagent;

import cn.ac.iie.cls.cc.slavecc.SlaveHandler;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

/**
 *
 * @author alexmu
 */
public class CLSAgentDownloadHandler implements SlaveHandler {

    static final String PROPERTIES_PATHNAME = "D:\\transclient\\properties\\controller.properties";
    static final String SRC_PATHNAME = "D:\\transclient";
    String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>"
            + "<requestParams>"
            + "<bsISIP>10.128.75.3</bsISIP>"
            + "</requestParams>";

    public String execute(String pRequestContent) {
        pRequestContent = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>"
                + "<requestParams>"
                + "<bsISIP>10.128.75.3</bsISIP>"
                + "</requestParams>";
        String result = null;
        try {
            Document doc = DocumentHelper.parseText(pRequestContent);
            Element rootElt = doc.getRootElement();
            Iterator iter = rootElt.elementIterator("bsISIP");
            String ipStr = "";
            while (iter.hasNext()) {
                Element bsElement = (Element) iter.next();
                ipStr = bsElement.getText();
            }
            if (modifyProperties("localIP", ipStr, PROPERTIES_PATHNAME)) {
                ;
            } else {
                result = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?><error><message>create agent fail：。。。</message></error>";
                return result;
            }

            Date date = new Date();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            String zipPathName = SRC_PATHNAME + sdf.format(date) + "-" + ipStr + ".zip";
            compress(SRC_PATHNAME, zipPathName);
            result = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?><response><downloadUri>http://10.128.68.49:7080/resources/clsagent/download/transclient" + sdf.format(date) + "-" + ipStr + ".zip" + "</downloadUri></response>";
        } catch (Exception e) {
            result = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?><error><message>create agent fail：。。。</message></error>";
            System.out.println("协同代理生成失败...." + e.getMessage());
        }
        return result;
    }

    //修改properties配置
    private boolean modifyProperties(String key, String value, String fileURL) {
        Properties prop = new Properties();
        File file = new File(fileURL);
        if (!file.exists()) {
            System.out.println(fileURL + "文件不存在！");
            return false;
        } else {
            try {
                InputStream fis = new FileInputStream(file);
                prop.load(fis);
                fis.close();
                FileOutputStream fos = new FileOutputStream(file);
                prop.setProperty(key, value);
                prop.store(fos, fileURL);
                fos.close();
            } catch (Exception ex) {
                return false;
            }
        }
        return true;
    }
    //zip压缩
    static final int BUFFER = 1024;

    public void compress(String srcPathName, String zipPathName) throws Exception {
        File zipFile = new File(zipPathName);
        File file = new File(srcPathName);
        if (!file.exists()) {
            throw new RuntimeException(srcPathName + "不存在！");
        }
        FileOutputStream fileOutputStream = new FileOutputStream(zipFile);
        CheckedOutputStream cos = new CheckedOutputStream(fileOutputStream,
                new CRC32());
        ZipOutputStream out = new ZipOutputStream(cos);
        String basedir = "";
        compress(file, out, basedir);
        out.close();
    }

    private void compress(File file, ZipOutputStream out, String basedir) throws Exception {
        /*
         * 判断是目录还是文件
         */
        if (file.isDirectory()) {
            this.compressDirectory(file, out, basedir);
        } else {
            this.compressFile(file, out, basedir);
        }
    }

    /**
     * 压缩一个目录
     */
    private void compressDirectory(File dir, ZipOutputStream out, String basedir) throws Exception {
        if (!dir.exists()) {
            return;
        }

        File[] files = dir.listFiles();
        for (int i = 0; i < files.length; i++) {
            /*
             * 递归
             */
            compress(files[i], out, basedir + dir.getName() + "/");
        }
    }

    /**
     * 压缩一个文件
     */
    private void compressFile(File file, ZipOutputStream out, String basedir) throws Exception {
        if (!file.exists()) {
            return;
        }
        BufferedInputStream bis = new BufferedInputStream(
                new FileInputStream(file));
        ZipEntry entry = new ZipEntry(basedir + file.getName());
        out.putNextEntry(entry);
        int count;
        byte data[] = new byte[BUFFER];
        while ((count = bis.read(data, 0, BUFFER)) != -1) {
            out.write(data, 0, count);
        }
        bis.close();
    }
//    public String execute(String pRequestContent) throws Exception {
//        String result = null;
//        return result;
//    }
}
