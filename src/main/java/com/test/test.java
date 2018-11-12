package com.test;

import com.table.BuildTable;
import com.weibo.Operation;
import org.junit.Test;

public class test {

    @Test
    public void createNamespace() {
        BuildTable.createNamespace();
    }

    @Test
    public void buildTable(){
    BuildTable.buildTable();
    }


    @Test
    public void tweeting(){
        Operation.tweeting("888", "hello 999 ");
    }

    @Test
    public void addAttend() {
        Operation.addAttends("999", "888");
    }
    @Test
    public void testShowMessage(){
        Operation.getWeiboFromAttends("999");
    }

}
