/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slavecc.dataetl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.XMLWriter;

/**
 *
 * @author alexmu
 */
public class DataProcessParser {

    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(DataProcessParser.class.getName());
    }

    public static Map parse(String pDataProcessDescriptor) {
        Map<String, String> dataProcessDescriptor = new HashMap<String, String>();
        try {
            Document document = DocumentHelper.parseText(pDataProcessDescriptor);
            Element operatorNode = document.getRootElement();
            XMLWriter xw1 = new XMLWriter(System.out);
            xw1.write(operatorNode);
//            xw1.close();

            Element elment = operatorNode.element("connect");
            System.out.println(elment);
            System.out.println(operatorNode.remove(elment));
//            List<Element> elements = operatorNode.elements();
//
//            for (Element element : elements) {
//                System.out.println("****" + element.getName() + "*******");
//                if (element.getName().equals("connect")) {
//                    System.out.println("delete:" + operatorNode.remove(element));
//                }
//            }
            XMLWriter xw2 = new XMLWriter(System.out);
            xw2.write(operatorNode);
//            xw2.close();
//            System.out.println("<<"+operatorNode.elements()+">>");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return dataProcessDescriptor;
    }

    public static void main(String[] args) {
        File inputXml = new File("dataprocess-specific.xml");
        try {
            String dataProcessDescriptor = "";
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(inputXml)));
            String line = null;
            while ((line = br.readLine()) != null) {
//                System.out.println(line);
                dataProcessDescriptor += line;
            }
//            System.out.println(dataProcessDescriptor);
            parse(dataProcessDescriptor);
            System.out.println("parse ok");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
