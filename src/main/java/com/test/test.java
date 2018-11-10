package com.test;

import com.table.BuildTable;
import com.weibo.Operation;
import org.junit.Test;

public class test {
    @Test
    public void buildTable(){
    BuildTable.buildTable();
    }

    @Test
    public void addUser(){

    }

    @Test
    public void tweeting(){
        Operation.tweeting("888","hello world "+System.currentTimeMillis());
        Operation.addAttends("888","124");
    }
    @Test
    public void testShowMessage(){
        Operation.getWeiboFromAttends("999");
    }
}
