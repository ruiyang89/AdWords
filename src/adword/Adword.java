package adword;

import java.io.IOException;
import java.sql.SQLException;
/**
 *
 * @author Ray
 */
public class Adword {

    public static void main(String[] args)throws SQLException,IOException, InterruptedException {
        
        Initialization init=new Initialization();
        String[] taskArgs=init.initialzeTask("E:\\Projects\\DBMS\\dbms\\system.in");
        /*init.createTables(); 
        Insert addData=new Insert(taskArgs[0],taskArgs[1]);
        addData.runInsert();*/
        firstGreedy task1=new firstGreedy(taskArgs[0],taskArgs[1],taskArgs[2]);
        task1.selectKeyword();
        /*secondGreedy task2=new secondGreedy(taskArgs[0],taskArgs[1],taskArgs[3]);
        task2.selectKeyword();*/
        /*firstBalance task3=new firstBalance(taskArgs[0],taskArgs[1],taskArgs[4]);
        task3.selectKeyword();
        secondBalance task4=new secondBalance(taskArgs[0],taskArgs[1],taskArgs[5]);
        task4.selectKeyword();
        firstGeneralized task5=new firstGeneralized(taskArgs[0],taskArgs[1],taskArgs[6]);
        task5.selectKeyword();
        secondGeneralized task6=new secondGeneralized(taskArgs[0],taskArgs[1],taskArgs[7]);
        task6.selectKeyword();*/
    }    
}