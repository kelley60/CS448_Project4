package query;

import global.AttrType;
import heap.HeapFile;
import parser.AST_Describe;
import relop.Schema;

/**
 * Execution plan for describing tables.
 */
class Describe implements Plan {

	//fileName of the table
	private String tableName;
	
	private Schema schema;
	
	private HeapFile heapFile;

	
  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if table doesn't exist
   */
  public Describe(AST_Describe tree) throws QueryException {
	  
	  tableName = tree.getFileName();
	  schema = QueryCheck.tableExists(tableName);
	  //heapfile that has table that needs to have values inserted
	  heapFile = new HeapFile(tableName);
	  
  } // public Describe(AST_Describe tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
	  
	  int length = schema.getCount();
	  for (int i = 0; i < length; i++){
		  String columnName = schema.fieldName(i);
		  String columnType = AttrType.toString(schema.fieldType(i));
		  System.out.println("Column name is " + columnName + " and column type is " + columnType);
	  }
	  

  } // public void execute()

} // class Describe implements Plan
