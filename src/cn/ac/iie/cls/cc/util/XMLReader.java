/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.util;

import cn.ac.iie.cls.cc.slave.dataetl.ETLJob;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

/**
 *
 * @author alexmu
 */
public class XMLReader {
    public static String value = "";//final value for key
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
    public static String getValueFromStrDGText(String xml, String name_key) {
        value = "";
        Document doc = null;
        SAXReader reader = new SAXReader();
        try {
            //String code = xmlPath.get
            InputStream in = new ByteArrayInputStream(xml.getBytes());
            doc = reader.read(in);
        } catch (DocumentException ex) {
            //logger.debug("read xml file error! " + ex.getLocalizedMessage() + "|" + ex.getMessage() + "|" + ex.getStackTrace());
            return "";
        }
        if (doc != null) {
            Element rootEl = doc.getRootElement();
            getElValueDGText(rootEl, name_key);

            if (value != null && !value.equals("")) {
                return value;
            }
        }
        return "";
    }

    private static void getElValueDGText(Element rootEl, String key) {
        if (rootEl.elements().size() > 0) {
            List<Element> childrenList = rootEl.elements();
            for (int i = 0; i < childrenList.size(); i++) {
                getElValueDGText(childrenList.get(i), key);
            }
        }
        String _name = rootEl.attributeValue("name");
        //logger.debug("xml'name: " + _name);
        if (_name != null && _name.equals(key)) {
            if (!value.equals("")) {
                value = value + "|" + rootEl.getText();
            } else {
                value = rootEl.getText();
            }
        }
    }
}
