package com.weibo;


import com.myutil.CloseUtil;
import com.table.BuildTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class Operation {
    //get a hbase configuration
    private static Configuration configuration = BuildTable.configuration;


    /**
     * tweeting
     * add data to content_table
     * add data into mail_table
     */
    public static void tweeting(String UID, String content) {
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
    public static void addAttends(String UID, String attendsUID) {
        HConnection connection = null;
        try {
            connection = HConnectionManager.createConnection(configuration);

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
            //将我关注的人的微博rowkey发送到我的mail
            sendWhenGetFan(UID,attendsUID);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            CloseUtil.CloseIO(connection);
        }
    }


    /**
     * 将我关注的人的微博发到我的邮箱
     * 每次点击关注时调用
     * 傻逼啊为什么要多同时发送多个啊我们一次不就关注一个么
     */
    public static void sendWhenGetFan(String myUID, String... myAttendsUID) {
        HConnection connection = null;
        try {
            connection = HConnectionManager.createConnection(configuration);
            HTableInterface hTableMail = connection.getTable(BuildTable.T_MAIL);
            HTableInterface hTableContent = connection.getTable(BuildTable.T_CONTENT);


            Scan scan = new Scan();
            //用于存放取出来的关注的人所发布的微博的rowkey
            List<byte[]> rowkeys = new ArrayList<byte[]>();

            for (String attend : myAttendsUID) {
                //过滤扫描rowkey，即：前置位匹配被关注的人的uid_
                //这一段不是很懂
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


                if (rowkeys.size() <= 0) return;
                HTable ContentTable = new HTable(configuration,BuildTable.T_CONTENT);
                Put mailPut = new Put(Bytes.toBytes(myUID));
                List<Put> mailPutList = new ArrayList<Put>();
                Get get = null;
                /*for(byte[] rk : rowkeys){
                    Put put = new Put(Bytes.toBytes(myUID));
                    //uid_timestamp
                    String rowKey = Bytes.toString(rk);
                    //借取uid
                    String attendUID = rowKey.substring(0, rowKey.indexOf("_"));
                    long timestamp = Long.parseLong(rowKey.substring(rowKey.indexOf("_") + 1));
                    //将微博rowkey添加到指定单元格中
                    put.add(Bytes.toBytes("info"), Bytes.toBytes(attendUID), timestamp, rk);
                    mailPutList.add(put);
                }*/

                for (byte[] contentRowKey : rowkeys){
                    get = new Get(contentRowKey);
                    get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("content"));
                    Result result1 = ContentTable.get(get);
                    for (Cell cell : result1.rawCells()){
                        mailPut.addColumn(Bytes.toBytes("info"),Bytes.toBytes(attend),contentRowKey);
                        mailPutList.add(mailPut);
                    }
                }
                hTableMail.put(mailPutList);

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            CloseUtil.CloseIO(connection);
        }
    }

   /* *//**
     * 将我关注的人的微博发到我的邮箱
     *//*
    public void sendWeiboToFans() {
        byte[] tRelationRokey = null;
        byte[] tRelationRokeyAttendsValue = null;
        HConnection connection = null;
        try {
            connection = HConnectionManager.getConnection(configuration);
            HTableInterface hTableMail = connection.getTable(BuildTable.T_MAIL);
            HTable relation = new HTable(configuration, BuildTable.T_RELATIONS);

            Scan relationScan = new Scan();
            ResultScanner relationScanResults = relation.getScanner(relationScan);
            for (Result relationScanResult : relationScanResults){
                for (Cell cell : relationScanResult.rawCells()){
                    tRelationRokey = CellUtil.cloneRow(cell);
                    Get
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            CloseUtil.CloseIO(connection);
        }
    }*/

    /**
     * 将我取消关注的人的UID从我的relation表的attends中移除
     * 将我的uid从我取消关注的人的relation表中的fans里移除
     * 将我的mail表中有关他的信息删除
     * */
    public static void removeAAttend(String myUID , String attendUID){
        HTable hTableRelation = null;
        HTable hTableMail = null;
        try {
            //更新relation表
            hTableRelation = new HTable(configuration,BuildTable.T_RELATIONS);
            Delete myUIDDelete = new Delete(Bytes.toBytes(myUID));
            myUIDDelete.addColumn(Bytes.toBytes("attends"),Bytes.toBytes(attendUID));
            Delete attendUIDDelete = new Delete(Bytes.toBytes(attendUID));
            attendUIDDelete.addColumn(Bytes.toBytes("fans"),Bytes.toBytes(myUID));
            hTableRelation.delete(myUIDDelete);
            hTableRelation.delete(attendUIDDelete);

            //更新mail
            hTableMail = new HTable(configuration,BuildTable.T_MAIL);
            Delete delete = new Delete(Bytes.toBytes(myUID));
            delete.addColumn(Bytes.toBytes("info"),Bytes.toBytes(attendUID));
            hTableMail.delete(delete);

        }catch (Exception e){
            e.printStackTrace();
        }finally {
        }

    }


    /**
     * 获取我关注的人的微博
     * */
    public static void getWeiboFromAttends(String myUID){
        try {
            HTable hTableMail = new HTable(configuration,BuildTable.T_MAIL);
            Get weiboRowkeyget = new Get(Bytes.toBytes(myUID));
            weiboRowkeyget.setMaxVersions(5);
            Result weiboRowkeyGetResult = hTableMail.get(weiboRowkeyget);
            List<byte[]> rowkeys = new ArrayList<byte[]>();
            for (Cell cell : weiboRowkeyGetResult.rawCells()){
                rowkeys.add(CellUtil.cloneValue(cell));
            }

            HTable hTableContent = new HTable(configuration,BuildTable.T_CONTENT);
            List<Get> weiboGets = new ArrayList<Get>();
            Get weiboGet = null;
            for (byte[] weiboRowkey : rowkeys){
                weiboGet = new Get(weiboRowkey);
                weiboGets.add(weiboGet);
            }

            List<String> contents = new ArrayList<String>();
            Result[] weiboGetResults = hTableContent.get(weiboGets);
            for (Result weiboGetResult : weiboGetResults){
                for (Cell cell : weiboGetResult.rawCells()){
                    contents.add(Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }

            for (String content : contents){
                System.out.println(content);
            }
        }catch (Exception e){

        }finally {
        }
    }

}
