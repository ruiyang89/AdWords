package adword;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class Initialization {
    private Connection conn;
    private String username;
    private String userpwd;
    
    //@ create tables
    public void createTables() throws SQLException, IOException{
        DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
        conn =DriverManager.getConnection ("jdbc:oracle:thin:@localhost:1521:orcl",username, userpwd);
        Statement stmt;
        stmt = conn.createStatement();
        stmt.execute("Create Table Queries(qid INTEGER, query VARCHAR(400), primary key (qid))");
        stmt.execute("Create Table Advertisers(advertiserId INTEGER, budget FLOAT, ctc FLOAT, primary key (advertiserid))");
        stmt.execute("Create Table Keywords(advertiserId INTEGER, keyword VARCHAR(100), bid FLOAT, primary key (advertiserid, keyword))");
        conn.close();
    }
    //@ return task parameters
    public String[] initialzeTask(String src)throws IOException{
        File file = new File(src);
        String[] result=new String[8];
        if (file.isFile() && file.exists()) {
            InputStreamReader read = new InputStreamReader(new FileInputStream(file));
            BufferedReader bufferedReader = new BufferedReader(read);
            String lineTXT;
            int count=0;
            while ((lineTXT = bufferedReader.readLine()) != null) {
                String[] line=lineTXT.split(" ");
                result[count]=line[line.length-1];
                count++;
            }
            read.close();
        }
        else{
            System.out.println("Can't find file!");
        }
        username=result[0];
        userpwd=result[1];
        return result;
    }
    
}
