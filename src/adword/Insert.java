package adword;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;


public class Insert {
    
    String username;
    String userpwd;
    
    public Insert(String username, String userpwd){
        this.username=username;
        this.userpwd=userpwd;
    }
    private void insertQueries() throws IOException{
                BufferedWriter bw = new BufferedWriter(new FileWriter("queries.ctl"));
                bw.write("LOAD DATA");
                bw.newLine();
                bw.write("INFILE Queries.dat");
                bw.newLine();
                bw.write("INTO TABLE Queries");
                bw.newLine();
                bw.write("FIELDS TERMINATED BY '\t'");
                bw.newLine();
                bw.write("(qid,query)");
                bw.close();
    }
     private void insertAdvertisers() throws IOException{
                BufferedWriter bw = new BufferedWriter(new FileWriter("advertisers.ctl"));
                bw.write("LOAD DATA");
                bw.newLine();
                bw.write("INFILE Advertisers.dat");
                bw.newLine();
                bw.write("INTO TABLE Advertisers");
                bw.newLine();
                bw.write("FIELDS TERMINATED BY '\t'");
                bw.newLine();
                bw.write("(advertiserid,budget,ctc)");
                bw.close();
    }
     
     private void insertKeywords() throws IOException{
                BufferedWriter bw = new BufferedWriter(new FileWriter("keywords.ctl"));
                bw.write("LOAD DATA");
                bw.newLine();
                bw.write("INFILE Keywords.dat");
                bw.newLine();
                bw.write("INTO TABLE Keywords");
                bw.newLine();
                bw.write("FIELDS TERMINATED BY '\t'");
                bw.newLine();
                bw.write("(advertiserid,keyword,bid)");
                bw.close();
    }
    
     private void runQueries(){
         try{
             insertQueries();
             String sqlldrCmd = "SQLLDR CONTROL=queries.ctl "+
                     "LOG=queries.log "+
                     "DATA=Queries.dat USERID="+username+"/"+userpwd+
                     " BAD=queries.bad ERRORS=999 "+
                     "LOAD=5000 SKIP=0";
             Runtime rt = Runtime.getRuntime();
             Process proc = rt.exec(sqlldrCmd);
         }
         catch (Exception e)
         {
             e.printStackTrace();
         }
     }
     
        private void runAdvertisers(){
         try{
             insertAdvertisers();
             String sqlldrCmd = "SQLLDR CONTROL=advertisers.ctl "+
                     "LOG=advertisers.log "+
                     "DATA=Advertisers.dat USERID="+username+"/"+userpwd+
                     " BAD=advertisers.bad ERRORS=999 "+
                     "LOAD=5000 SKIP=0";
             Runtime rt = Runtime.getRuntime();
             Process proc = rt.exec(sqlldrCmd);
         }
         catch (Exception e)
         {
             e.printStackTrace();
         }
     }
        
        private void runKeywords(){
         try{
             insertKeywords();
             String sqlldrCmd = "SQLLDR CONTROL=keywords.ctl "+
                     "LOG=keywords.log "+
                     "DATA=keywords.dat USERID="+username+"/"+userpwd+
                     " BAD=Keywords.bad ERRORS=999 "+
                     "LOAD=5000 SKIP=0";
             Runtime rt = Runtime.getRuntime();
             Process proc = rt.exec(sqlldrCmd);
         }
         catch (Exception e)
         {
             e.printStackTrace();
         }
     }
     
     public void runInsert() throws InterruptedException, IOException{
         runQueries();
         runAdvertisers();
         runKeywords();
     }
     
}