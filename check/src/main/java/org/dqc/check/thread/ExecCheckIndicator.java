package org.dqc.check.thread;

import org.dqc.check.dao.MysqlDAO;
import org.apache.log4j.Logger;

public class ExecCheckIndicator implements Runnable {

    private static final Logger logger = Logger.getLogger(ExecCheckIndicator.class);
    public String ds;
    public String indicator;
    public ExecCheckIndicator(String ds, String indicator){
        this.ds=ds;
        this.indicator=indicator;
    }

    public void run(){
        try{
            new MysqlDAO().insertCheckRstIndicator(ds,indicator);
        }catch (Exception e){
            logger.info(e.toString());
        }
    }
}
