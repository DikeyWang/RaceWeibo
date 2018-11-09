package com.table;


import com.myutil.CloseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

public class BuildTable {

    //获取配置信息
    private Configuration configuration = HBaseConfiguration.create();
    //表名的二进制数据

    public static final byte[] T_RELATIONS = Bytes.toBytes("weibo_relation");
    public static final byte[] T_CONTENT = Bytes.toBytes("weibo_content");
    public static final byte[] T_MAIL = Bytes.toBytes("weibo_mail");

    /**
     * 构建命名空间
     * name：weibo
     * author：Dikey
     * createTime：系统当前时间
     */
    public void initTable() {
        HBaseAdmin admin = null;
        try {
            //获取admin
            admin = new HBaseAdmin(configuration);
            //构建命名空间描述信息快
            NamespaceDescriptor weibo = NamespaceDescriptor.create("weibo")         //name of the namespace
                    .addConfiguration("Author", "Dikey")                             //just <k,v>
                    .addConfiguration("CreateTime", System.currentTimeMillis() + "")   //just <k,v>
                    .build();                                                       //so now get it
            //通过admin来创建命名空间
            admin.createNamespace(weibo);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != admin) {
                //自写工具类，用于关闭io
                CloseUtil.CloseIO(admin);
            }
        }
    }

    /**
     * 表存在则删除
     */
    public boolean notExistOrDelete(byte[]... tableNames) {
        boolean flag = true;
        HBaseAdmin admin = null;
        for (byte[] tableName : tableNames) {
            try {
                admin = new HBaseAdmin(configuration);
                if (admin.tableExists(tableName)) {
                    System.out.println("表“" + tableName + "”已存在，将删除该表重新创建！！！");
                    //删除表之前需要先disable
                    admin.disableTable(tableName);
                    admin.deleteTable(tableName);
                }
            } catch (Exception e) {
                flag = false;
                e.printStackTrace();
            } finally {
                CloseUtil.CloseIO(admin);
            }
        }
        return flag;
    }

    /**
     * 创建微博内容表
     * Table Name:weibo_content
     * RowKey:用户ID_时间戳
     * ColumnFamily:info
     * 好像很麻烦，其实不麻烦
     * ColumnLabel:标题   内容   图片URL《---这什么玩意儿？我还得给他创建数据库？算了保存到本地吧<--本项目只考虑内容
     * Version:1个版本
     */
    public void createContentTable() {
        //自写函数判断表是否存在，存在则删除表重新建立
        notExistOrDelete(T_CONTENT);
        HBaseAdmin admin = null;
        try {
            admin = new HBaseAdmin(configuration);
            //创建表描述
            HTableDescriptor contentTableDescriptor = new HTableDescriptor(TableName.valueOf(T_CONTENT));
            //创建列族描述
            HColumnDescriptor info = new HColumnDescriptor(Bytes.toBytes("info"));

            //以下设置表，参数复制
            //设置块缓存
            info.setBlockCacheEnabled(true);
            //设置块缓存大小 so why 2097152? is it 256m?
            info.setBlocksize(2097152);
            //设置压缩方式 here we don`t need it
            //info.setCompressionType(Algorithm.SNAPPY);
            //设置版本确界  for version
            info.setMaxVersions(1);
            info.setMinVersions(1);

            //将列族描述添加至表描述
            contentTableDescriptor.addFamily(info);
            //通过admin创建contentTableDescriptor所描述的表
            admin.createTable(contentTableDescriptor);
            System.out.println("表“" + T_CONTENT + "”创建成功！！！");

        } catch (Exception e) {
            System.out.println("error发生于BuildTable.createContentTable()");
            e.printStackTrace();
        } finally {
            if (null != admin) {
                CloseUtil.CloseIO(admin);
            }
        }
    }

    /**
     * 用户关系表,
     * Table Name:weibo_relations,
     * RowKey:用户ID,
     * ColumnFamily:attends,fans,
     * ColumnLabel:关注用户ID，粉丝用户ID,
     * ColumnValue:用户ID,
     * Version：1个版本.
     */
    public void creteRelationTable() {
        //自写函数判断表是否存在，存在则删除表重新建立
        notExistOrDelete(T_RELATIONS);

        HBaseAdmin admin = null;
        try {
            //创建admin
            admin = new HBaseAdmin(configuration);
            //创建关系表表描述
            HTableDescriptor relationDescriptor = new HTableDescriptor(TableName.valueOf(T_RELATIONS));
            //创建列族attends和fans的描述
            HColumnDescriptor attendsDescriptor = new HColumnDescriptor(Bytes.toBytes("attends"));
            HColumnDescriptor fansDescriptor = new HColumnDescriptor(Bytes.toBytes("fans"));

            //设置块缓存
            attendsDescriptor.setBlockCacheEnabled(true);
            //设置块缓存大小
            attendsDescriptor.setBlocksize(2097152);
            //设置压缩方式
            //info.setCompressionType(Algorithm.SNAPPY);
            //设置版本确界
            attendsDescriptor.setMaxVersions(1);
            attendsDescriptor.setMinVersions(1);

            fansDescriptor.setBlockCacheEnabled(true);
            fansDescriptor.setBlocksize(2097152);
            fansDescriptor.setMinVersions(1);
            fansDescriptor.setMaxVersions(1);

            //将列族描述加入表描述
            relationDescriptor.addFamily(attendsDescriptor);
            relationDescriptor.addFamily(fansDescriptor);
            //通过admin创建表
            admin.createTable(relationDescriptor);
            System.out.println("表“" + T_RELATIONS + "”创建成功！！！");

        } catch (Exception e) {
            System.out.println("error发生于createRelationTable()");
            e.printStackTrace();
        } finally {
            if (null != admin) {
                CloseUtil.CloseIO(admin);
            }
        }
    }

    /**
     * 设计好的 拿来用
     * 创建微博收件箱表
     * Table Name: weibo_mail
     * RowKey:用户ID
     * ColumnFamily:info
     * ColumnLabel:用户ID-发布微博的人的用户ID
     * ColumnValue:关注的人的微博的RowKey
     * Version:1000
     */

    public void createMailTable() {
        //自写函数判断表是否存在，存在则删除表重新建立
        notExistOrDelete(T_MAIL);
        HBaseAdmin admin = null;
        try {
            admin = new HBaseAdmin(configuration);
            //get mailtable descriptor
            HTableDescriptor mailDescriptor = new HTableDescriptor(TableName.valueOf(T_MAIL));
            //get family descriptor
            HColumnDescriptor info = new HColumnDescriptor(Bytes.toBytes("info"));

            info.setBlockCacheEnabled(true);
            info.setBlocksize(256 * 1024 * 8);
            // why not min = 1 , max = 1000 ???
            info.setMinVersions(1000);
            info.setMaxVersions(1000);

            //add famillydescriptor into tabledescriptor
            mailDescriptor.addFamily(info);
            //creat table by admin
            admin.createTable(mailDescriptor);
            System.out.println("表“" + T_MAIL + "”创建成功！！！");

        } catch (Exception e) {
            System.out.println("error happen at createMailTable");
            e.printStackTrace();
        } finally {
            CloseUtil.CloseIO(admin);
        }
    }

}
