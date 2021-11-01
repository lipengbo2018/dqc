package org.dqc.check.thread;

import org.dqc.check.dao.MysqlDAO;
import org.apache.log4j.Logger;

public class ExecCheck implements Runnable {

    private static final Logger logger = Logger.getLogger(ExecCheck.class);
    public String[] paramarr;
    public ExecCheck(String[] paramarr){
        this.paramarr=paramarr;
    }

    public void run(){
        try{
            new MysqlDAO().insertCheckRst(paramarr);
        }catch (Exception e){
            logger.info(e.toString());
        }
    }
}
