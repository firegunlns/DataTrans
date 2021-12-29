package com.lns.learn;

import com.sun.org.apache.bcel.internal.generic.ACONST_NULL;

import javax.xml.crypto.Data;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class DataTrans {
    HashMap<String, DataSource> dataSourceHashMap = new HashMap<>();

    HashMap<Integer, String> map_mysql = new HashMap<Integer, String>() {
        {
            put(Types.BIGINT, "bigint");
            put(Types.BINARY, "blob");
            put(Types.BIT, "bit");
            put(Types.BLOB, "longblob");
            put(Types.BOOLEAN, "bit");
            put(Types.CHAR, "char(?)");
            put(Types.CLOB, "longtext");
            put(Types.DATALINK, "varchar(?)");
            put(Types.DATE, "date");
            put(Types.DECIMAL, "decimal(?,?)");
            put(Types.DOUBLE, "double");
            put(Types.FLOAT, "float");
            put(Types.INTEGER, "int");
            put(Types.LONGNVARCHAR, "longtext");
            put(Types.LONGVARBINARY, "longblob");
            put(Types.NCHAR, "char(?)");
            put(Types.NCLOB, "longtext");
            put(Types.NUMERIC, "decimal(?,?)");
            put(Types.REAL, "float");
            put(Types.ROWID, "int");
            put(Types.SMALLINT, "smallint");
            put(Types.SQLXML, "longtext");
            put(Types.TIME, "time");
            put(Types.TIME_WITH_TIMEZONE, "time");
            put(Types.TIMESTAMP, "datetime");
            put(Types.TINYINT, "tinyint");
            put(Types.VARBINARY, "blob");
            put(Types.VARCHAR, "varchar(?)");
            put(Types.LONGVARCHAR, "longtext");
        }
    };

    public String readCmd(String hint){
        return readCmd(hint, true);
    }

    public String readCmd(String hint, String defVal){
        String ret = readCmd(hint, true);
        if ((ret == null) || ret.length() ==0)
            ret = defVal;

        return defVal;
    }

    public String readLine() throws Exception{
        if (System.console() != null)
            return System.console().readLine();

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        return reader.readLine();
    }

    public String readPassword() throws Exception{
        if (System.console() != null)
            return System.console().readPassword().toString();

        return  this.readLine();
    }

    public String readCmd(String hint, boolean toLower){
        try {
            // print the prompt
            System.out.print("DT> ");
            System.out.print(hint);

            // read command
            BufferedReader rd = new BufferedReader(new InputStreamReader(System.in));
            String cmdLine = rd.readLine().trim().replaceAll("\\s", " ");
            if (toLower)
                cmdLine = cmdLine.toLowerCase();

            return cmdLine;
        }catch (Exception e){
            e.printStackTrace();
        }

        return "";
    }

    public void run(){
        System.out.println("Data transmitter v1.0");
        if (System.console() != null){
            String line = System.console().readLine();
            System.out.println(line);
        }

        while(true){
            try {
                String cmdLine = readCmd("");

                if (cmdLine.equals("help") || cmdLine.equals("h") || cmdLine.equals("?") ){
                    // print command help
                    System.out.println("usage:");
                    System.out.println("help | ? | h: print this usage.");
                    System.out.println("db type: list all Databases types current supported.");
                    System.out.println("add conn: define a new database connection by specify the DB type、host/ip、port、username、password.");
                    System.out.println("list conn: list all the conn and their status.");
                    System.out.println("show schema $conn_name]: describe one database connected by the conn.");
                    System.out.println("show tables $conn_name $schema_name: describe one database connected by the conn.");
                    System.out.println("copy schema: copy a table's schema from source to desc.");
                    System.out.println("copy table: copy a table's rows from source to desc.");
                    System.out.println("quit: quit data trans.");
                    continue;
                }

                if (cmdLine.equals("quit"))
                    break;

                if (cmdLine.equals("db type")){
                    System.out.println("current supported database: oracle, mysql");
                    continue;
                }

                if (cmdLine.equals("add conn")){
                    String db_type = readCmd("please input the type of the db(default mysql):", "mysql");
                    String host = readCmd("please input the hostname or ip of the database's host(default localhost):", "localhost");
                    String port = readCmd("please input the port(default 3306):", "3306");
                    String username = readCmd("please input username(default root):", "root");
                    System.out.print("please input password: ");
                    String password = readPassword().toString();
                    String name = readCmd("please name the connection: ");

                    DataSource ds = new DataSource();
                    ds.setDBtype(db_type);
                    ds.setUser(username);
                    ds.setPassword(password);
                    ds.setHost(host);
                    ds.setPort(Integer.valueOf(port));
                    ds.setName(name);

                    String test = readCmd("will you try connect the database?(yes/no)", "yes");
                    if (test.equals("yes") || test.equals("y")){
                        Connection conn = ds.connect();
                        if (conn != null) {
                            System.out.println("connect to datasource ok!");
                            conn.close();
                        }
                        else
                            System.out.println("connect to datasource failed!");
                    }

                    dataSourceHashMap.put(ds.getName(), ds);
                    continue;
                }

                if (cmdLine.equals("list conn")){
                    System.out.printf("There are %d connections:\n", dataSourceHashMap.size());
                    Iterator<String> it = dataSourceHashMap.keySet().iterator();
                    int no = 1;
                    while(it.hasNext()){
                        String name = it.next();
                        DataSource ds = dataSourceHashMap.get(name);

                        System.out.printf("%d: name: %s, type: %s, host: %s, port: %d\n", no ++, ds.getName(), ds.getDBType(), ds.getHost(), ds.getPort());
                    }
                    continue;
                }

                if (cmdLine.startsWith("show schema ")){
                    String ds = cmdLine.replace("show schema ", "");
                    DataSource dataSource = dataSourceHashMap.get(ds);
                    if (dataSource != null) {
                        List<String> names = listSchema(dataSource);
                        System.out.printf("there are %d schemas/databases in connection %s:\n", names.size(), ds);
                        int no = 1;
                        for (String name: names){
                            System.out.printf("%d: %s\n", no, name);
                            no ++;
                        }
                    }
                    else
                        System.out.println("no such connection");

                    continue;
                }

                if (cmdLine.startsWith("show tables ")){
                    String cmd = cmdLine.replace("show tables ", "");
                    String[] args = cmd.split(" ");
                    String ds = args[0];
                    String schema = args[1];

                    DataSource dataSource = dataSourceHashMap.get(ds);
                    if (dataSource != null) {
                        List<String> names = listTables(dataSource, schema);;
                        System.out.printf("there are %d tables in schema %s:\n", names.size(), schema);
                        int no = 1;
                        for (String name: names){
                            System.out.printf("%d: %s\n", no, name);
                            no ++;
                        }
                    }
                    else
                        System.out.println("no such connection");

                    continue;
                }
            }catch(Exception e){
                e.printStackTrace();
            }
        }

    }

    public static void main(String[] args) {
        new DataTrans().run();
    }

    public String convertType(DataSource ds, int typ){
        if (ds.getDBType().equals("mysql")) {
            String dstType = map_mysql.get(typ);
            return dstType;
        }

        return null;
    }

    public boolean copyTableDef(DataSource src, String srcTab, DataSource dst){
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            String srcConnStr = String.format("jdbc:mysql://%s:%d/", src.getHost(), src.getPort());
            Connection srcConn = DriverManager.getConnection(srcConnStr, src.getUser(), src.getPassword());

            String dstConnStr = String.format("jdbc:mysql://%s:%d/", dst.getHost(), dst.getPort());
            Connection dstConn = DriverManager.getConnection(dstConnStr, dst.getUser(), dst.getPassword());

            ResultSet col_rs = srcConn.getMetaData().getColumns(src.getDatabase(), null, srcTab, null );
            StringBuffer col_defs = new StringBuffer();

            while(col_rs.next()){
                // add a "," for each column def except the first one
                if (col_defs.length() != 0)
                    col_defs.append(", ");

                String col_def = "";
                String col_name = col_rs.getString("COLUMN_NAME");
                String col_typename = col_rs.getString("TYPE_NAME");
                String col_nullable = col_rs.getString("IS_NULLABLE");
                String col_remarks = col_rs.getString("REMARKS");
                String col_defaults = col_rs.getString("COLUMN_DEF");
                int col_size = col_rs.getInt("COLUMN_SIZE");
                int decimal_digits = col_rs.getInt("DECIMAL_DIGITS");
                String is_autoinc = col_rs.getString("IS_AUTOINCREMENT");


                int col_type = col_rs.getInt("DATA_TYPE");
                String col_dst_type = convertType(dst, col_type);
                col_dst_type = col_dst_type.replaceFirst("\\?",  new Integer(col_size).toString());
                col_dst_type = col_dst_type.replaceFirst("\\?", new Integer(decimal_digits).toString());

                col_def = col_name + " " + col_dst_type + " ";
                if (col_defaults != null)
                    if (col_defaults.length() > 0)
                        col_def += " default " + col_defaults + " ";

                if (col_nullable.equals("NO"))
                    col_def += " not null ";

                if (is_autoinc.equals("YES"))
                    col_def += " primary key auto_increment ";

                if (col_remarks != null)
                    col_def += "comment '" + col_remarks + "' ";

                col_defs.append(col_def);
            }

            String create_sql = String.format("create table %s (%s)", dst.getDatabase() + "." + srcTab, col_defs);
            PreparedStatement ps = dstConn.prepareStatement(create_sql);
            ps.execute();
            ps.close();

            dstConn.close();
            srcConn.close();
        }catch (Exception e){
            e.printStackTrace();
        }

        return true;
    }

    public boolean copyTable(DataSource src, String srcTab, DataSource dst){
        try{
            Class.forName("com.mysql.cj.jdbc.Driver");
            String srcConnStr = String.format("jdbc:mysql://%s:%d/", src.getHost(), src.getPort());
            Connection srcConn = DriverManager.getConnection(srcConnStr, src.getUser(), src.getPassword());

            String dstConnStr = String.format("jdbc:mysql://%s:%d/?rewriteBatchedStatements=true", dst.getHost(), dst.getPort());
            Connection dstConn = DriverManager.getConnection(dstConnStr, dst.getUser(), dst.getPassword());

            // prepare the destination insert sql
            ResultSet col_rs = srcConn.getMetaData().getColumns(src.getDatabase(), null, srcTab, null );
            String col_names = "";
            String col_values = "";
            int col_num = 0;
            while(col_rs.next()){
                col_names += "," + col_rs.getString("COLUMN_NAME");
                col_values += ",?";
                col_num ++;
            }

            col_names = col_names.replaceFirst(",", "(");
            col_values = col_values.replaceFirst(",", "(");
            String insertSql = String.format("insert into %s %s) values %s)", dst.getDatabase() + "." + srcTab, col_names, col_values);

            // start select and insert
            PreparedStatement ps_src = srcConn.prepareStatement("select * from " + src.getDatabase() + "." + srcTab);
            PreparedStatement ps_dst = dstConn.prepareStatement(insertSql);

            ResultSet rs = ps_src.executeQuery();
            ResultSetMetaData meta = rs.getMetaData();

            int batch_num = 10000;
            int cnt = 0;
            while(rs.next()){
                for (int i = 1; i <= col_num; i ++){
                    ps_dst.setObject(i, rs.getObject(i));
                }
                ps_dst.addBatch();
                cnt ++;
                if (cnt % batch_num == 0){
                    ps_dst.executeBatch();
                }
            }

            if (cnt % batch_num != 0){
                ps_dst.executeBatch();
            }

            rs.close();
            ps_src.close();
            ps_dst.close();

            srcConn.close();
            dstConn.close();

            return true;
        }catch (Exception e){
            e.printStackTrace();
        }

        return false;
    }

    public List<String> listTables(DataSource ds, String schema){
        try{
            Class.forName("com.mysql.cj.jdbc.Driver");
            String connStr = String.format("jdbc:mysql://%s:%d/",
                    ds.getHost(), ds.getPort());
            Connection conn = DriverManager.getConnection(connStr, ds.getUser(), ds.getPassword());

            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet rs = metaData.getTables(schema, null, null, null);
            List<String> tables = new ArrayList<>();
            while(rs.next()) {
                tables.add(rs.getString("TABLE_NAME"));
                //System.out.printf("%s, %s, %s\r\n", rs.getString("TABLE_CAT"), rs.getString("TABLE_NAME"), rs.getString("TABLE_TYPE"));
            }
            rs.close();
            conn.close();
            return tables;
        }catch (Exception e){
            e.printStackTrace();
        }

        return null;
    }

    public List<String> listSchema(DataSource ds){
        try{
            String connStr = "";
            if (ds.getDBType().equals("mysql")) {
                Class.forName("com.mysql.cj.jdbc.Driver");
                connStr = String.format("jdbc:mysql://%s:%d/",
                        ds.getHost(), ds.getPort());
            }
            else if (ds.getDBType().equals("oracle")){
                Class.forName("oracle.jdbc.driver.OracleDriver");
                connStr = String.format("jdbc:oracle:thin:@%s:%d:orcl",
                        ds.getHost(), ds.getPort());
            }

            Connection conn = DriverManager.getConnection(connStr, ds.getUser(), ds.getPassword());

            DatabaseMetaData metaData = conn.getMetaData();

            ResultSet rs = null;
            if (ds.getDBType().equals("mysql"))
                rs = metaData.getCatalogs();
            else
                rs = metaData.getSchemas();

            List<String> names = new ArrayList<>();
            while(rs.next()) {
                names.add(rs.getString(1));
            }

            rs.close();
            conn.close();

            return names;
        }catch (Exception e){
            e.printStackTrace();
        }

        return null;
    }
}
