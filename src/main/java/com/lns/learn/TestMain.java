package com.lns.learn;

import org.junit.Test;

import java.util.List;

public class TestMain {
    @Test
    public void test1(){
        System.out.println("test data trans");
        DataSource src = new DataSource();
        src.setDBtype("oracle");
        src.setHost("8.142.155.185");
        src.setPort(1521);
        src.setUser("system");
        src.setPassword("Q1w2e3r4");
        src.setDatabase("scott");

        DataSource dst = new DataSource();
        dst.setDBtype("mysql");
        dst.setHost("127.0.0.1");
        dst.setPort(3306);
        dst.setUser("root");
        dst.setPassword("R4e3w2q1!@");
        dst.setDatabase("test2");

        DataTrans dt = new DataTrans();

        List<String> schemas = dt.listSchema(src);
        System.out.println("----------------------------------------------");
        System.out.println("databases:");
        for (String name : schemas){
            System.out.println("\t" + name);
        }

        System.out.println("----------------------------------------------");
        List<String> tables = dt.listTables(src, "mysql");
        System.out.println("mysql tables:");
        for (String name : tables){
            System.out.println("\t" + name);
        }

        System.out.println("----------------------------------------------");
        System.out.println("copy table ");
        long l0 = System.currentTimeMillis();
        dt.copyTableDef(src, "commodity", dst);
        dt.copyTableDef(src, "t1", dst);
        dt.copyTableDef(src, "t2", dst);
        dt.copyTableDef(src, "t3", dst);
        dt.copyTable(src, "commodity", dst);
        dt.copyTable(src, "t1", dst);
        dt.copyTable(src, "t2", dst);
        dt.copyTable(src, "t3", dst);
        long l1 = System.currentTimeMillis();
        System.out.printf("time used is %d ms\n", l1 - l0);
    }
}
