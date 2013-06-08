/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slavecc.dataetl;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
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

    public static final String CLS_AGENT_DATA_COLLECT_DESC = "clsAgentDataCollectDesc";
    public static final String DATA_ETL_DESC = "dataEtlDesc";
    public static final String HDFS_FILE_INPUT_OPERATOR_DESC =
            "<operator name=\"hdfs_file_input_operator\" class=\"HdfsFileInputOperator\" version=\"1.0\" x=\"-1\" y=\"-1\">\n"
            + "	<parameter name=\"srcpath\">hdfs:///mnt/data/</parameter>\n"
            + "</operator>";
    public static final String HDFS_FILE_INPUT_CONNECT_DESC = "<connect from=\"hdfs_file_input_operator.outport1\" to=\"DEST_OPERATOR_PORT\"/>";
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(DataProcessParser.class.getName());
    }

    private static String getXmlString(Element pOperatorNode) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        XMLWriter xw = new XMLWriter(bos);
        xw.write(pOperatorNode);
        xw.close();
        xw = null;
        return new String(bos.toByteArray(),"UTF-8");
    }

    public static Map parse(String pDataProcessDescriptor) {
        Map<String, String> dataProcessDescriptor = new HashMap<String, String>();
        try {
            Document document = DocumentHelper.parseText(pDataProcessDescriptor);
            Element operatorNode = document.getRootElement();

            String operatorNodeXmlStr = getXmlString(operatorNode);
            logger.debug("data process descriptor:" + operatorNodeXmlStr);

            List<Element> elements = operatorNode.elements("operator");

            int clsAgentDataCollectOperatorDescCnt = 0;
            String clsAgentDataCollectOperatorName = "";
            for (Element element : elements) {
                if (element.attributeValue("class").startsWith("GetherDataFrom")) {
                    dataProcessDescriptor.put(CLS_AGENT_DATA_COLLECT_DESC, getXmlString(element));
                    clsAgentDataCollectOperatorName = element.attributeValue("name");

                    operatorNode.remove(element);
                    Document hdfsFileInputOperatorDoc = DocumentHelper.parseText(HDFS_FILE_INPUT_OPERATOR_DESC);
                    operatorNode.add(hdfsFileInputOperatorDoc.getRootElement());

                    clsAgentDataCollectOperatorDescCnt++;
                }
            }

            if (clsAgentDataCollectOperatorDescCnt != 1) {
                throw new Exception("the descriptor'number of data collector  of cls agent in data process descriptor:" + operatorNodeXmlStr + " is " + clsAgentDataCollectOperatorDescCnt + ",which is not correct");
            }

            elements = operatorNode.elements("connect");
            int clsAgentDataCollectConnectCnt = 0;
            for (Element element : elements) {
                if (element.attributeValue("from").startsWith(clsAgentDataCollectOperatorName)) {
                    operatorNode.remove(element);
                    Document hdfsFileInputOperatorConnectDoc = DocumentHelper.parseText(HDFS_FILE_INPUT_CONNECT_DESC.replace("DEST_OPERATOR_PORT", element.attributeValue("to")));
                    operatorNode.add(hdfsFileInputOperatorConnectDoc.getRootElement());

                    clsAgentDataCollectConnectCnt++;
                }
            }

            if (clsAgentDataCollectConnectCnt < 1) {
                throw new Exception("the connector'number of data collector of cls agent in data process descriptor:" + operatorNodeXmlStr + " is less than 1");
            }
            dataProcessDescriptor.put(DATA_ETL_DESC, getXmlString(operatorNode));
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
            System.out.println(dataProcessDescriptor);
            parse(dataProcessDescriptor);
            System.out.println("parse ok");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
