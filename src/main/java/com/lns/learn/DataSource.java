package com.lns.learn;

import java.sql.Connection;
import java.sql.DriverManager;

public class DataSource {
    String DBtype;
    String host;
    int port;
    String user;
    String password;
    String database;
    String name;

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    String status;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDBType() {
        return DBtype;
    }

    public void setDBtype(String DBtype) {
        this.DBtype = DBtype;
    }
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    Connection connect() {
        if (getDBType().equals("mysql")) {
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
                String srcConnStr = String.format("jdbc:mysql://%s:%d/", getHost(), getPort());
                Connection srcConn = DriverManager.getConnection(srcConnStr, getUser(), getPassword());
                return srcConn;
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        } else if (getDBType().equals("oracle")) {
            try {
                Class.forName("oracle.jdbc.driver.OracleDriver");
                String connStr = String.format("jdbc:oracle:thin:@%s:%d:orcl", getHost(), getPort());
                Connection srcConn = DriverManager.getConnection(connStr, getUser(), getPassword());
                return srcConn;
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }

        return null;
    }
}
