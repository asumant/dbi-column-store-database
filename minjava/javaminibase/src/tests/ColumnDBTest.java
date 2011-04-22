package tests;


	
public class ColumnDBTest {
	public static void main (String argv[]) {

	     DBDriver dbt = new DBDriver();
	     boolean dbstatus;

	     dbstatus = dbt.runAllTests();

	     if (dbstatus != true) {
	       System.err.println ("Error encountered during buffer manager tests:\n");
	       Runtime.getRuntime().exit(1);
	     }

	     Runtime.getRuntime().exit(0);
	   }
    
     
}
  
		

