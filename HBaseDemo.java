package HBaseDemo;

import java.io.BufferedReader;

import java.io.IOException;

import java.io.InputStreamReader;

import java.util.Scanner;



import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.Cell;

import org.apache.hadoop.hbase.CellUtil;

import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.HColumnDescriptor;

import org.apache.hadoop.hbase.HTableDescriptor;

import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.hbase.client.Admin;

import org.apache.hadoop.hbase.client.Connection;

import org.apache.hadoop.hbase.client.ConnectionFactory;

import org.apache.hadoop.hbase.client.Delete;

import org.apache.hadoop.hbase.client.Put;

import org.apache.hadoop.hbase.client.Result;

import org.apache.hadoop.hbase.client.ResultScanner;

import org.apache.hadoop.hbase.client.Scan;

import org.apache.hadoop.hbase.client.Table;

import org.apache.hadoop.hbase.util.Bytes;

class HBaseDemo {
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;

    //建立连接
    public static void init(){
        configuration  = HBaseConfiguration.create();
        configuration.set("hbase.rootdir","hdfs://localhost:9000/hbase");
        try{
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    //关闭连接
    public static void close(){
        try{
            if(admin != null){
                admin.close();
            }
            if(null != connection){
                connection.close();
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }


    public static void createTable(String tableName,
                                   String[] fields) throws IOException {

        init();
        TableName tablename = TableName.valueOf(tableName);
        if(admin.tableExists(tablename)){
            System.out.println("表已存在，将执行删除原表，重建新表!");
            admin.disableTable(tablename);
            admin.deleteTable(tablename);//删除原来的表
        }
        HTableDescriptor hTableDescriptor =
                new HTableDescriptor(tableName);
        for(String str:fields){
            HColumnDescriptor hColumnDescriptor =
                    new HColumnDescriptor(str);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        admin.createTable(hTableDescriptor);
        System.out.println("创建成功");
        close();
    }



    public static void addRecord(String tableName,
                                 String rowKey,String []fields,
                                 String [] values) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        for (int i = 0; i < fields.length; i++) {
            Put put = new Put(rowKey.getBytes());
            String [] cols = fields[i].split(":");
            if(cols.length==1)
            {
                put.addColumn(cols[0].getBytes(),
                        "".getBytes(), values[i].getBytes());//因为当输入的是单列族，split仅读出一个字符字符串，即cols仅有一个元素

            }
            else {
                put.addColumn(cols[0].getBytes(),
                        cols[1].getBytes(), values[i].getBytes());
            }

            table.put(put);

        }
        table.close();
        close();

    }



    /**

     * 根据表名查找表信息

     */

    public static void getData(String tableName)throws  IOException{
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for(Result result:scanner)
        {
            showCell((result));
        }
        close();
    }

    /**

     * 格式化输出

     * @param result

     */

    public static void showCell(Result result){
        Cell[] cells = result.rawCells();
        for(Cell cell:cells){
            System.out.println("RowName(行键):"+new String(CellUtil.cloneRow(cell))+" ");
            System.out.println("Timetamp(时间戳):"+cell.getTimestamp()+" ");
            System.out.println("column Family（列簇）:"+new String(CellUtil.cloneFamily(cell))+" ");
            System.out.println("column Name（列名）:"+new String(CellUtil.cloneQualifier(cell))+" ");
            System.out.println("value:（值）"+new String(CellUtil.cloneValue(cell))+" ");
            System.out.println();
        }

    }



    public static void scanColumn (String tableName,
                                   String column) throws IOException
    {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        String [] cols = column.split(":");
        if(cols.length==1)
        {
            scan.addFamily(Bytes.toBytes(column));
        }
        else {
            scan.addColumn(Bytes.toBytes(cols[0]),Bytes.toBytes(cols[1]));
        }
        ResultScanner scanner = table.getScanner(scan);
        for (Result result = scanner.next(); result !=null;result = scanner.next()) {
            showCell(result);
        }
        table.close();
        close();
    }



    public static void modifyData(String tableName,String rowKey,String column,
                                  String value) throws IOException
    {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowKey.getBytes());
        String [] cols = column.split(":");
        if(cols.length==1)
        {
            put.addColumn(column.getBytes(),
                    "".getBytes() , value.getBytes());//qualifier:列族下的列名
        }
        else {
            put.addColumn(cols[0].getBytes(),
                    cols[1].getBytes() , value.getBytes());//qualifier:列族下的列名
        }
        table.put(put);
        table.close();
        close();
    }


    public static void deleteRow(String tableName,
                                 String rowKey) throws IOException
    {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(rowKey.getBytes());
        table.delete(delete);
        table.close();
        close();
    }



    /**

     * @param args

     * @throws IOException

     */

    public static void main(String[] args) throws IOException {
        HBaseDemo test_Two = new HBaseDemo();
        boolean flag =true;
        while(flag)
        {
            System.out.println("------------------------------------------------提供以下功能----------------------------------------------");
            System.out.println("                       1- createTable（创建表  ,提供表名、列族名）                                      ");
            System.out.println("                       2-addRecord （向已知表名、行键、列簇的表添加值）                       ");
            System.out.println("                       3- ScanColumn（浏览表     某一列的数据）                                            ");
            System.out.println("                       4- modifyData（修改某表   某行，某一列，指定的单元格的数据）    ");
            System.out.println("                       5- deleteRow（删除 某表   某行的记录）                                                 ");
            System.out.println("------------------------------------------------------------------------------------------------------------------");
            Scanner scan = new Scanner(System.in);
            Integer choose1=scan.nextInt();

            switch (choose1) {
                case 1:
                {
                    System.out.println("请输入要创建的表名");
                    String tableName=scan.nextLine();
                    System.out.println("请输入要创建的表的列族个数");
                    int Num=scan.nextInt();
                    String [] fields = new String[Num];
                    System.out.println("请输入要创建的表的列族");
                    /* Scanner scanner = new Scanner(System.in);     scanner.next 如不是全局，即会记得上一次输出。相同地址读入值时*/
                    for(int i=0;i< fields.length;i++)
                    {
                        /*fields[i]=scan.next(); 因为之前没有输入过，所以可以读入新值*/
                        scan = new Scanner(System.in);
                        fields[i]=scan.nextLine();
                    }
                    System.out.println("正在执行创建表的操作");
                    test_Two.createTable(tableName,fields);
                    break;
                }

                case 2:
                {
                    System.out.println("请输入要添加数据的表名");
                    String tableName=scan.nextLine();
                    System.out.println("请输入要添加数据的表的行键");
                    String rowKey=scan.nextLine();
                    System.out.println("请输入要添加数据的表的列的个数");
                    int num =scan.nextInt();
                    String fields[]=new String[num];
                    System.out.println("请输入要添加数据的表的列信息 共"+num+"条信息");
                    for(int i=0;i< fields.length;i++)
                    {
                        BufferedReader in3= new BufferedReader(new InputStreamReader(System.in));
                        fields[i] = in3.readLine();
                        /*fields[i]=scan.next(); 因为之前没有输入过，所以可以读入新值*/
                    }
                    System.out.println("请输入要添加的数据信息 共"+num+"条信息");
                    String values[]=new String[num];
                    for(int i=0;i< values.length;i++)
                    {
                        BufferedReader in2 = new BufferedReader(new InputStreamReader(System.in));
                        values[i] = in2.readLine();
                    }
                    System.out.println("原表信息");
                    test_Two.getData(tableName);
                    System.out.println("正在执行向表中添加数据的操作........\n");
                    test_Two.addRecord(tableName, rowKey, fields, values);
                    System.out.println("\n添加后的表的信息........");
                    test_Two.getData(tableName);
                    break;
                }

                case 3:
                {
                    System.out.println("请输入要查看数据的表名");
                    String tableName=scan.nextLine();
                    System.out.println("请输入要查看数据的列名");
                    String column=scan.nextLine();
                    System.out.println("查看的信息如下：........\n");
                    test_Two.scanColumn(tableName, column);
                    break;
                }

                case 4:
                {
                    System.out.println("请输入要修改数据的表名");
                    String tableName=scan.nextLine();
                    System.out.println("请输入要修改数据的表的行键");
                    String rowKey=scan.nextLine();
                    System.out.println("请输入要修改数据的列名");
                    String column=scan.nextLine();
                    System.out.println("请输入要修改的数据信息  ");
                    String value=scan.nextLine();
                    System.out.println("原表信息如下：........\n");
                    test_Two.getData(tableName);
                    System.out.println("正在执行向表中修改数据的操作........\n");
                    test_Two.modifyData(tableName, rowKey, column, value);
                    System.out.println("\n修改后的信息如下：........\n");
                    test_Two.getData(tableName);
                    break;
                }

                case 5:
                {
                    System.out.println("请输入要删除指定行的表名");
                    String tableName=scan.nextLine();
                    System.out.println("请输入要删除指定行的行键");
                    String rowKey=scan.nextLine();
                    System.out.println("原表信息如下：........\n");
                    test_Two.getData(tableName);
                    System.out.println("正在执行向表中删除数据的操作........\n");
                    test_Two.deleteRow(tableName, rowKey);
                    System.out.println("\n删除后的信息如下：........\n");
                    test_Two.getData(tableName);
                    break;
                }

                default:
                {
                    System.out.println("   你的操作有误 ！！！    ");
                    break;
                }
            }

            System.out.println(" 你要继续操作吗？ 是-true 否-false ");
            flag=scan.nextBoolean();
        }
        System.out.println("   程序已退出！    ");
    }

}
