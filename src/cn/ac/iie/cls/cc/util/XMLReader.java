/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.util;

import cn.ac.iie.cls.cc.slave.dataetl.ETLJob;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;

/**
 *
 * @author alexmu
 */
public class XMLReader {
    public static String getXMLContent(String pXmlFilePathStr){
        
        File xmlFile = new File(pXmlFilePathStr);
        try {
            String xmlContent = "";
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(xmlFile)));
            String line = null;
            while ((line = br.readLine()) != null) {
                xmlContent += line;
            }
            return xmlContent;
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }
}
