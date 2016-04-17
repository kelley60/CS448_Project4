package query;

import java.util.ArrayList;

import global.Minibase;
import global.SortKey;
import heap.HeapFile;
import parser.AST_Select;
import relop.FileScan;
import relop.Iterator;
import relop.Predicate;
import relop.Projection;
import relop.Schema;
import relop.Selection;
import relop.SimpleJoin;

/**
 * Execution plan for selecting tuples.
 */
class Select implements Plan {
	
	//protected String fileName;
	//protected Predicate[][] pred;
	//protected Schema schema;
	//protected String[] cols;
	protected Integer[] fldnos;
	//protected String[] tables;
	protected SortKey[] sortkeys;
	protected HeapFile heapFile;
	static protected ArrayList<Iterator> selList;
	static protected ArrayList<Iterator> joinList;
	static protected ArrayList<Iterator> tableList;
		protected Projection root;

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if validation fails
   */
  public Select(AST_Select tree) throws QueryException {
	  
	  //initialize
	  
	  
	  
	String[] tables = tree.getTables(); 
	//THIS AS WELL
	Predicate[][] pred = tree.getPredicates();
	//QueryCheck.predicates(schema, pred);
	 
	String[] cols = tree.getColumns();
	//for(int i = 0; i < cols.length; i++){
	//	fldnos[i]= QueryCheck.columnExists(schema, cols[i]);
	//}
	
	//tree.g
	this.sortkeys = tree.getOrders();
	
	
	//create the tree
	
	//FileScan scan = new FileScan(this.schema, heapFile);
	
	//get selections
	//ArrayList<Selection> selList = new ArrayList<Selection>();
	ArrayList<Iterator> tableJoinList = new ArrayList<Iterator>();
	ArrayList<Schema> schemaList = new ArrayList<Schema>();
	//Boolean canSelect = true;
	
	//get the tables
	for(int i = 0; i < tables.length; i++){
		Schema tempSchema = Minibase.SystemCatalog.getSchema(tables[i]);
		
		//check if able to select
		/*for(int j = 0; j < pred.length; j++){
			for(int k = 0; k < pred[j].length; k++){
				if(!pred[j][k].validate(tempSchema)){
					canSelect = false;
					break;
				}
			}
			if(canSelect){
				HeapFile tempFile = new HeapFile(tables[i]);
				FileScan temp = new FileScan(tempSchema, tempFile);
				Selection sel = new Selection(temp,pred[j]); 
				selList.add(sel);
			}
		}*/
		HeapFile tempFile = new HeapFile(tables[i]);
		FileScan temp = new FileScan(tempSchema, tempFile);
		schemaList.add(tempSchema);
		//tableJoinList.add(temp);
		tableList.add(temp);
	}
	
	//cross join the tables together (for now)
	//need to determine the order of the joins later

	boolean beginJoin = false;
	
	int tableSize = tableList.size();
	int tableIndex = 2;
	int joinIndex = 0;
	int count = 0;
	ArrayList<Schema> joinSchema = new ArrayList<Schema>();
	//Schema nSchema = new Schema(0);

	while(count < tableSize - 1){
		if(beginJoin == false){
			//join the first 2 tables together
			SimpleJoin curJoin = new SimpleJoin(tableJoinList.get(0), tableJoinList.get(1));
			joinList.add(curJoin); // the first join
			Schema nSchema = Schema.join(tableJoinList.get(0).getSchema(), tableJoinList.get(1).getSchema());
			joinSchema.add(nSchema);
			beginJoin = true;
			count++;
		}
		else{
			//join the rest with the next table and the previous join iterator
			//last schema on joinSchema List should be the final schema
			SimpleJoin curJoin2 = new SimpleJoin(tableList.get(tableIndex), joinList.get(joinIndex));
			joinList.add(curJoin2);
			Schema nSchema2 = Schema.join(tableList.get(tableIndex).getSchema(), schemaList.get(joinIndex));
			joinSchema.add(nSchema2);
			tableIndex++;
			joinIndex++;
			count++;
		}
	}
	
	//select from the join
	//ArrayList<Iterator> prevSel = new ArrayList<Iterator>();
	
	int selCount;
	boolean beginSel = false;
	int prevSel = 0;
	for(int i = 0; i < pred.length; i++){
		if(beginSel == false){
			//select using the last join
			Selection tempSel = new Selection(joinList.get(joinList.size()-1),pred[i]);
			selList.add(tempSel);
		}
		else{
			//use the previous selects to create the next join
			Selection tempSel2 = new Selection(selList.get(prevSel),pred[i]);
			selList.add(tempSel2);
			prevSel++;
		}
	}
	
	//finally project with the last selection of the list
	
	for(int i = 0; i < cols.length; i++){
		fldnos[i] = QueryCheck.columnExists(joinSchema.get(joinSchema.size()-1), cols[i]);
	}
	this.root = new Projection(selList.get(selList.size()-1),fldnos);
	
	
	
	
  } // public Select(AST_Select tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
    // print the output message
	  root.execute();
	  System.out.println("0 rows affected. (Not implemented)");
    
  } // public void execute()

} // class Select implements Plan
