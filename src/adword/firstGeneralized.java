package adword;
import java.io.IOException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.sql.*;
import java.text.DecimalFormat;

public class firstGeneralized {
    private final String username;
    private final String userpwd;
    private final int slots;
    Connection conn;
    OutputStream out;
    
    public firstGeneralized(String username,String userpwd,String slots)throws SQLException,IOException{
        this.username=username;
        this.userpwd=userpwd;
        this.slots=Integer.parseInt(slots);
        DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
        conn =DriverManager.getConnection ("jdbc:oracle:thin:@localhost:1521:orcl",this.username, this.userpwd);
        File f = new File("system.out.5");
        out = new FileOutputStream(f,true);
    }
    
    
    public void selectKeyword()throws SQLException,IOException{
        Statement stmt=null;
        stmt = conn.createStatement();
        stmt.execute("Create Table Ad (advertiserId INTEGER, budget FLOAT, ctc FLOAT,adCount INTEGER default 0)");
        stmt.execute("Insert into Ad （advertiserid,budget,ctc）SELECT * FROM Advertisers");
        stmt.execute("Create or replace view adnum as "
                   + "select T.advertiserid, Ad.budget, T.ad_times "
                   + "from (select advertiserid, count(*) as ad_times from keywords group by advertiserid) T, Advertisers Ad "
                   + "where Ad.advertiserid=T.advertiserid");
        stmt.execute("create table res(advertiserid Integer, total float, key_times Integer, ad_times Integer, budget float, newbudget float, rank Integer default 0)");
        stmt.execute("create table bidtable(advertiserid integer, bid float)");
        stmt.execute("create table bidTotal(advertiserid integer, total float, rank Integer default 0)");
        ResultSet rs=stmt.executeQuery("SELECT * FROM QUERIES ORDER BY QID ASC");
        while(rs.next()){
            String query=rs.getString("query");
            int qid= rs.getInt("qid");
            selectBid(query,qid);
        }
        stmt.execute("drop view adnum");
        stmt.execute("drop table res");
        stmt.execute("drop table bidtable");
        stmt.execute("drop table AD");
        stmt.execute("drop table bidTotal");
        
        stmt.close();
        out.close();
        conn.close();
    }
    
    public void selectBid(String inputStr, int qid)throws SQLException, IOException{
        
        int count=0;
        Statement cstmt;
        PreparedStatement stmt;
        
        cstmt = conn.createStatement();
        cstmt.execute("create or replace type tabletype as table of VARCHAR(255) ");
        cstmt.execute("CREATE OR REPLACE FUNCTION split (p_list CLOB, p_sep VARCHAR := ' ') "
                + "RETURN tabletype PIPELINED IS l_idx PLS_INTEGER; v_list VARCHAR(255) := p_list; "
                + "BEGIN LOOP l_idx := INSTR (v_list, p_sep); IF l_idx > 0 "
                + "THEN PIPE ROW (SUBSTR (v_list, 1, l_idx - 1)); v_list := SUBSTR (v_list, l_idx + LENGTH (p_sep)); "
                + "ELSE PIPE ROW (v_list); "
                + "EXIT; END IF; END LOOP; END; ");
        cstmt.close();
        
        stmt = conn.prepareStatement("select *from table(split(?,' '))");
        stmt.setString(1,inputStr);
        ResultSet rs1=stmt.executeQuery();
        String queryStr;  
        while(rs1.next()){
        PreparedStatement stmt1;
        String key=rs1.getString("column_value");
        queryStr ="Insert into bidtable "
                + "Select Ad.advertiserid, K.bid "
                + "From Ad, Keywords K "
                + "Where Ad.advertiserid=K.advertiserid and K.keyword=?";
         stmt1 = conn.prepareStatement(queryStr);
         stmt1.setString(1,key);
         stmt1.executeUpdate();
         stmt1.close();
        }
        stmt.close();
        
        cstmt = conn.createStatement();
        cstmt.execute("Insert into bidTotal select advertiserid, total, rownum from (select advertiserid, sum(bid) as total from (select distinct *  from bidtable) group by advertiserid order by total desc, advertiserid asc)");
        cstmt.execute("delete from bidTable R1 where R1.advertiserid =(select distinct R2.advertiserid from bidTotal R2 where R2.total>(select R3.budget from AD R3 where R3.advertiserid=R2.advertiserid) and R2.advertiserid=R1.advertiserid )");
        cstmt.close();
        
        String query ="insert into res "
                    + "select advertiserid,total,key_times,ad_times,budget,newbudget, ROWNUM "
                    + "from(select T1.advertiserid, T2.total, T1.key_times, adnum.ad_times, adnum.budget, ad.budget as newbudget "
                         + "from (select advertiserid, count(*) as key_times from bidtable group by advertiserid) T1, "
                         + "(select advertiserid, sum(bid) as total from （select distinct * from bidtable) group by advertiserid ) T2, "
                         + "adnum, Ad "
                         + "where T1.advertiserid=adnum.advertiserid and T1.advertiserid=T2.advertiserid  and adnum.advertiserid=Ad.advertiserid and T2.total<=Ad.budget "
                         + "Order by T2.total*(1-EXP(-ad.budget/adnum.budget))*Ad.ctc*T1.key_times/SQRT(adnum.ad_times) desc) "
                    + "WHERE ROWNUM <=? ORDER BY ROWNUM ASC";
        stmt = conn.prepareStatement(query);
        stmt.setInt(1, slots);
        stmt.executeUpdate();
        stmt.close();
            
        cstmt = conn.createStatement();
        cstmt.execute("Update Ad Set ad.adcount=MOD((ad.adcount+1),100) where Ad.advertiserid in (select advertiserid from res)");
        cstmt.execute("Update Ad Set Ad.budget=(select Ad.budget-res.total from res where Ad.advertiserid=res.advertiserid) where Ad.advertiserid in (select advertiserid from res) and Ad.adcount<=Ad.ctc*100 and Ad.adcount>=1");  
        cstmt.close();
        
        stmt = conn.prepareStatement("select res.advertiserid,res.budget,Ad.budget as balance from Ad,res where Ad.advertiserid=res.advertiserid order by res.rank asc");
        ResultSet rs=stmt.executeQuery();
        while(rs.next()){
            int advertiserid=rs.getInt("advertiserid");
            float budget=rs.getFloat("budget");
            float balance=rs.getFloat("balance");
            DecimalFormat myFormatter = new DecimalFormat("##0.0#");
            String result=String.format("%d, %d, %d, %s, %s",qid,count+1,advertiserid,myFormatter.format(balance),myFormatter.format(budget));
            out.write(result.getBytes());
            out.write('\r'); 
            count++;
        }
        stmt.close();
        
        cstmt = conn.createStatement();
        cstmt.execute("delete from bidTotal");
        cstmt.execute("delete from bidtable");
        cstmt.execute("delete from res");
        cstmt.close();
      
    }
    
}
