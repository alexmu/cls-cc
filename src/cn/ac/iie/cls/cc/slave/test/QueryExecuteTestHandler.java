/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.test;

import cn.ac.iie.cls.cc.slave.SlaveHandler;
import cn.ac.iie.cls.cc.slave.linkagecmd.QueryExecuteHandler;
import cn.ac.iie.cls.cc.util.XMLReader;

/**
 *
 * @author root
 */
public class QueryExecuteTestHandler implements SlaveHandler {

    @Override
    public String execute(String pRequestContent) {
        String dataProcessDesc = XMLReader.getXMLContent("query-specific.xml");
        QueryExecuteHandler queryExecuteHandler = new QueryExecuteHandler();
        return queryExecuteHandler.execute(dataProcessDesc);
    }
}
