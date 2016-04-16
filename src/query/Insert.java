package query;

import global.Minibase;
import heap.HeapFile;
import parser.AST_Insert;
import relop.Schema;
import relop.Tuple;

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
	  //heapfile that has table that needs to have values inserted
	  heapFile = new HeapFile(tableName);

  } // public Insert(AST_Insert tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
	  
		 Tuple tuple = new Tuple(schema);
		 tuple.setAllFields(values);
		 tuple.insertIntoFile(heapFile);

    // print the output message
    System.out.println("1 rows affected.");

  } // public void execute()

} // class Insert implements Plan
