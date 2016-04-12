package query;

import global.Minibase;
import heap.HeapFile;
import parser.AST_Insert;
import relop.Schema;

/**
 * Execution plan for inserting tuples.
 */
class Insert implements Plan {

	private String tableName;
	
	private Object[] values;
	
	private Schema schema;
	
	private HeapFile heapFile;
	
  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if table doesn't exists or values are invalid
   */
  public Insert(AST_Insert tree) throws QueryException {
	  
	  tableName = tree.getFileName();
	  schema = QueryCheck.tableExists(tableName);
	  values = tree.getValues();
	  QueryCheck.insertValues(schema, values);
	  heapFile = new HeapFile(tableName);

  } // public Insert(AST_Insert tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
	  
	 for (int i = 0; i < values.length; i++){
		 heapFile.insertRecord((byte[]) values[i]);
	 }

    // print the output message
    System.out.println("1 rows affected.");

  } // public void execute()

} // class Insert implements Plan
