package net.wouto.modelsync.postgresql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PostgreSQLConnection {

    private Connection dbConnection;
    private String host;
    private String database;
    private String username;
    private String password;
    private int port;

    public PostgreSQLConnection(String host, int port) {
        this.host = host;
        this.port = port;
        this.dbConnection = null;
    }
    
    public PostgreSQLConnection(String host, int port, String database) {
        this(host, port);
        this.database = database;
    }
    
    public void setAuthentication(String username, String password) {
        this.username = username;
        this.password = password;
    }
    
    public void setPort(int port) {
        this.port = port;
    }
    
    public int getPort() {
        return this.port;
    }

    public void connect() {
        try {
            String conStr = "jdbc:postgresql://" + host + ":" + port + "/" + (database == null ? "" : database);
            
            Class.forName("org.postgresql.Driver");
            String username = (this.username == null ? "" : this.username);
            String password = (this.password == null ? "" : this.password);
            dbConnection = DriverManager.getConnection(conStr, username, password);
        } catch (ClassNotFoundException | SQLException ex) {
            dbConnection = null;
            Logger.getLogger(PostgreSQLConnection.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void disconnect() {
        if (this.dbConnection != null) {
            try {
                this.dbConnection.close();
            } catch (SQLException ex) {
                Logger.getLogger(PostgreSQLConnection.class.getName()).log(Level.SEVERE, null, ex);
            }
            this.dbConnection = null;
        }
    }

    public boolean isConnected() {
        if (this.dbConnection == null) {
            return false;
        }
        try {
            if (this.dbConnection.isClosed()) {
                return false;
            }
        } catch (SQLException ex) {
            Logger.getLogger(PostgreSQLConnection.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
            return this.dbConnection.isValid(1);
        } catch (SQLException ex) {
            Logger.getLogger(PostgreSQLConnection.class.getName()).log(Level.SEVERE, null, ex);
        }
        return false;
    }

    public Connection getConnection() {
        return this.dbConnection;
    }
    
}
