package com.weibo;


import com.myutil.CloseUtil;
import com.table.BuildTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.conf.ConfigurationManager;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import javax.sound.sampled.Line;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class Operation {
    //get a hbase configuration
    private Configuration configuration = HBaseConfiguration.create();

    /**
     * tweeting
     * add data to content_table
     * add data into mail_table
     */
    public void tweeting(String UID, String content) {
        HConnection connection = null;
        try {
            //get a connection from hbase
            connection = HConnectionManager.createConnection(configuration);
            //get T_CONTENT table
            HTableInterface hTableContent = connection.getTable(TableName.valueOf(BuildTable.T_CONTENT));

            //now we can build a rowKey
            long timeStamp = System.currentTimeMillis();
            String rowKey = UID + "_" + timeStamp;
            //new we need a "put"
            //first get a put by rowKey
            Put put = new Put(Bytes.toBytes(rowKey));
            //add data into put
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("content"), Bytes.toBytes(content));
            //so now we can put this table to the HBASE
            hTableContent.put(put);

            //获取粉丝信息，以便将动态发送到每个粉丝
            HTableInterface hTableRelation = connection.getTable(BuildTable.T_RELATIONS);

            //get当前UID的fans
            Get get = new Get(Bytes.toBytes(UID));
            get.addFamily(Bytes.toBytes("fans"));

            //现在得到所有的信息
            Result result = hTableRelation.get(get);

            List<byte[]> fans = new ArrayList<byte[]>();
            //将信息转为行，然后遍历得到列信息，此处既是fans
            for (Cell cell : result.rawCells()) {
                fans.add(CellUtil.cloneQualifier(cell));
            }
            //没有粉丝就算了
            if (fans.size() == 0) return;


            HTableInterface hTableMail = connection.getTable(BuildTable.T_MAIL);
            List<Put> puts = new ArrayList<Put>();
            //将fan的信息放入puts里等下迭代
            for (byte[] fan : fans) {
                Put fanput = new Put(fan);
                fanput.addColumn(Bytes.toBytes("info"), Bytes.toBytes(UID), timeStamp, Bytes.toBytes(rowKey));
                puts.add(fanput);
            }
            //原来可以迭代put，学到了
            hTableMail.put(puts);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            CloseUtil.CloseIO(connection);
        }
    }


    /**
     * 1.将我关注的人的UID放到我的attends里
     * 2.将我的UID放到我关注的人的fans里
     */
    public void addAttends(String UID, String attendsUID) {
        HConnection connection = null;
        try {
            connection = HConnectionManager.getConnection(configuration);

            //操作关系表

            HTableInterface hTableRelation = connection.getTable(TableName.valueOf(BuildTable.T_RELATIONS));

            Put addAttendsPut = new Put(Bytes.toBytes(UID));

            //byte[] attendB = Bytes.toBytes(attend);
            //将我关注的人的UID放到我的attends里，并将put放入puts里
            addAttendsPut.addColumn(Bytes.toBytes("attends"), Bytes.toBytes(attendsUID), Bytes.toBytes(UID));
            hTableRelation.put(addAttendsPut);
            //将我的uid添加到我关注的人的fans中
            Put attendsAddFansPut = new Put(Bytes.toBytes(attendsUID));
            attendsAddFansPut.addColumn(Bytes.toBytes("fans"), Bytes.toBytes(UID), Bytes.toBytes(attendsUID));
            hTableRelation.put(attendsAddFansPut);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            CloseUtil.CloseIO(connection);
        }
    }


    /**
     * 将我关注的人的微博发到我的邮箱
     * 每次点击关注时调用
     */
    public void sendWhenGetFan(String myUID, String... myAttends) {
        HConnection connection = null;
        try {
            connection = HConnectionManager.getConnection(configuration);
            HTableInterface hTableMail = connection.getTable(BuildTable.T_MAIL);
            HTableInterface hTableContent = connection.getTable(BuildTable.T_CONTENT);


            Scan scan = new Scan();
            //用于存放取出来的关注的人所发布的微博的rowkey
            List<byte[]> rowkeys = new ArrayList<byte[]>();

            for (String attend : myAttends) {
                //过滤扫描rowkey，即：前置位匹配被关注的人的uid_
                RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(attend + "_"));
                //为扫描对象指定过滤规则
                scan.setFilter(filter);
                //通过扫描对象得到scanner
                ResultScanner result = hTableContent.getScanner(scan);
                //迭代器遍历扫描出来的结果集
                Iterator<Result> iterator = result.iterator();
                while (iterator.hasNext()) {
                    //取出每一个符合扫描结果的那一行数据
                    Result r = iterator.next();
                    for (Cell cell : r.rawCells()) {
                        //将得到的rowkey放置于集合容器中
                        rowkeys.add(CellUtil.cloneRow(cell));
                    }
                }

            }

            if (rowkeys.size() <= 0) return;
            HTable ContentTable = new HTable(configuration,BuildTable.T_CONTENT);
            Put put = new Put(Bytes.toBytes(myUID));
            Get get = null;
            for (byte[] contengRowKey : rowkeys){
                get = new Get(contengRowKey);
                get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("content"));
                Result result = ContentTable.get(get);
                for (Cell cell : result.rawCells()){
                    put.addColumn(Bytes.toBytes("info"),Bytes.toBytes(""))
                }
            }

            /*Put put = null;
            for (String myAttend : myAttends){
                put = new Put(Bytes.toBytes(myUID));

                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes(myAttend),Bytes.toBytes())
            }
            */

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            CloseUtil.CloseIO(connection);
        }
    }

    /**
     * 将我关注的人的微博发到我的邮箱
     * 第一次启动系统时调用
     */
    public void sendWeiboToFans() {
        HConnection connection = null;
        try {
            connection = HConnectionManager.getConnection(configuration);
            HTableInterface hTableMail = connection.getTable(BuildTable.T_MAIL);
            HTable relation = new HTable(configuration, BuildTable.T_RELATIONS);


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            CloseUtil.CloseIO(connection);
        }
    }

}
