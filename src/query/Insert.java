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

	//fileName of the table
	private String tableName;
	
	private Object[] values;
	
	private Schema schema;
	
	private HeapFile heapFile;
	
	private IndexDesc[] indexes;
	
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
		 
		 indexes = Minibase.SystemCatalog.getIndexes(tableName);
		 for (int i = 0; i < indexes.length; i++){
			 IndexDesc tempIndexDesc = indexes[i];
			 String columnName = tempIndexDesc.columnName;
			 String tableName = tempIndexDesc.tableName;
			 String indexName = tempIndexDesc.indexName;
			 
			 Minibase.SystemCatalog.dropIndex(indexName);
			 Minibase.SystemCatalog.createIndex(indexName, tableName, columnName);
		 }
		 
    // print the output message
    System.out.println("1 row inserted.");

  } // public void execute()

} // class Insert implements Plan
