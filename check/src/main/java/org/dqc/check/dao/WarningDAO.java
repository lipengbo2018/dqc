package org.dqc.check.dao;

import org.dqc.check.util.ConfigLoader;
import org.dqc.check.util.DefaultConfigLoader;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;

public class WarningDAO {

    private static final Logger logger = Logger.getLogger(WarningDAO.class);

    public DefaultConfigLoader cfg;
    public WarningDAO() throws ClassNotFoundException
            , NoSuchMethodException, InstantiationException, IllegalAccessException
            , InvocationTargetException {
        this.cfg=new ConfigLoader().getConfigInfo();
    }



}
