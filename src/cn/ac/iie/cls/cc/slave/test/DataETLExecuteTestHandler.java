/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.test;

import cn.ac.iie.cls.cc.slave.SlaveHandler;
import cn.ac.iie.cls.cc.slave.dataetl.DataETLExecuteHandler;
import cn.ac.iie.cls.cc.util.XMLReader;

/**
 *
 * @author alexmu
 */
public class DataETLExecuteTestHandler implements SlaveHandler {

    @Override
    public String execute(String pRequestContent) {
        String dataProcessDesc = XMLReader.getXMLContent("simple-dataprocess-specific.xml");
        DataETLExecuteHandler dataETLExecuteHandler = new DataETLExecuteHandler();
        return dataETLExecuteHandler.execute(dataProcessDesc);
    }
    
}
