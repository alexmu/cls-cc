/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slavecc.dataetl;

import cn.ac.iie.cls.cc.slavecc.SlaveHandler;

/**
 *
 * @author alexmu
 */
public class DataETLExecuteHandler implements SlaveHandler,Runnable {

    private static DataETLExecuteHandler dataETLExecuteHandler = null;
    
    @Override
    public String execute(String pRequestContent) throws Exception {
        String result = null;

        result = pRequestContent != null && !pRequestContent.isEmpty() ? pRequestContent : this.toString();
        return "hello";
    }
    
    @Override
    public void run(){
        
    }

    public static void main(String[] args) {
    }
}
