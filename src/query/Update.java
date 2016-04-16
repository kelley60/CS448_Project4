package query;

import global.Minibase;
import global.RID;
import heap.HeapFile;
import parser.AST_Update;
import relop.FileScan;
import relop.Predicate;
import relop.Schema;
import relop.Tuple;

/**
 * Execution plan for updating tuples.
 */
class Update implements Plan {

	private String tableName;
	
	private Object[] values;
	
	private Schema schema;
	
	private HeapFile heapFile;
	
	private Predicate[][] pred;
	
	private String columns[];
	
	private int length;
	
	private int[] fieldNumbers;
	
	
  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if invalid column names, values, or pedicates
   */
  public Update(AST_Update tree) throws QueryException {

	  this.tableName = tree.getFileName();
	  this.heapFile	= new HeapFile(tableName);
	  this.schema = QueryCheck.tableExists(tableName);
	  this.pred = tree.getPredicates();
	  this.length = pred.length;
	  this.columns = tree.getColumns();  
	  this.values = tree.getValues();
	  
	  QueryCheck.insertValues(schema, values); 
	  QueryCheck.predicates(schema, pred);
	  for (int i = 0; i < columns.length; i++){
		  QueryCheck.columnExists(schema, columns[i]);
	  }
	  
	  fieldNumbers = new int[columns.length];
	  for (int i = 0; i < columns.length; i++){
		  fieldNumbers[i] = schema.fieldNumber(columns[i]);
	  }
	  QueryCheck.updateValues(schema, fieldNumbers, values);

	  
  } // public Update(AST_Update tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {

	  IndexDesc[] inds = Minibase.SystemCatalog.getIndexes(tableName);
	  //RID rid = Minibase.SystemCatalog.getFileRID(tableName, true);
	  //byte[] b = heapFile.selectRecord(rid);
	  
	  FileScan scan = new FileScan(schema, heapFile);
	  
	  while(scan.hasNext()){
		  boolean pass = true;
		  Tuple tup = scan.getNext();
		  //evaluate tuple
		  for(int i = 0; i < pred.length; i++){
			  for(int j = 0; j < pred[i].length; j++){
				  if(!pred[i][j].evaluate(tup)){
					  pass = false;
				  }
			  }
		  }
		  //if passes all pred, update the tuple/record
		  if(pass == true){
			  RID rid = scan.getLastRID();
			  Tuple newTuple = tup;
			  for (int i = 0; i < values.length; i++){
				  newTuple.setField(columns[i], values[i]);
			  }			  
			  byte[] newRecord = newTuple.getData();
			  heapFile.updateRecord(rid, newRecord);
		  }
	  }
		 
	  
    // print the output message
    System.out.println("Row(s) updated.");

  } // public void execute()

} // class Update implements Plan
