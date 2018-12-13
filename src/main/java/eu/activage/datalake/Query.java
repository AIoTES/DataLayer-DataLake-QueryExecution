package eu.activage.datalake;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.util.TablesNamesFinder;

public class Query {
	String index;
	String[] columns;
	String [] conditions;
	private final Logger logger = LoggerFactory.getLogger(Query.class);
	
	Query(String sql) throws Exception{
		// Parse query
		Statement stmt = CCJSqlParserUtil.parse(sql);
	    		
		// Get data
		if(stmt instanceof Select){
			Select select = (Select) stmt;
		    				
			// Get table names
			// Table name = platform
			index = getTableNames(select).get(0); // Only one table
			logger.debug("Index: " + index);
			
			// Get column names
			columns = getColumnNames(select); // TODO: add support to expressions
            logger.debug("Column names: " + columns);
			
			// Get conditions
            conditions = getAndConditions(select); // TODO: add more condition operators
            logger.debug("AND conditions: " + conditions);
          				
		}else{
			throw new Exception("Query not supported: " + sql);
		}
	
		
	}
	
   // Query parse methods
    
    protected List<String> getTableNames(Select select){
    	TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
    	List<String> tableList = tablesNamesFinder.getTableList(select);
    	return tableList;
    }
    
    protected String[] getColumnNames(Select select) throws Exception{	
    	PlainSelect plain=(PlainSelect)select.getSelectBody();     
        List selectitems=plain.getSelectItems();
        logger.debug(selectitems.size() + " query items identified");
        String[] columns = new String[selectitems.size()];
        for(int i=0;i<selectitems.size();i++){
           Expression expression=((SelectExpressionItem) selectitems.get(i)).getExpression();  
           logger.debug("Expression:-"+expression);
           if( expression instanceof Column){
               // A column name
        	   Column col=(Column)expression;
               logger.debug(col.getTable()+","+col.getColumnName());      
               columns[i] = col.getColumnName();
               // Do something with col.getTable() to support queries over multiple tables

           }else if (expression instanceof Function){
        	   // An expression
               Function function = (Function) expression;
               logger.debug(function.getAttribute()+","+function.getName()+""+function.getParameters()); 
               // TODO
               throw new Exception("Expressions are not supported");
           }
        }
    	
    	return columns;
    }
    
    protected String[] getAndConditions(Select select){
    	// Get conditions with AND
    	List<String> conditions = new ArrayList<String>();
    	PlainSelect ps = (PlainSelect) select.getSelectBody();
    	
    	Expression expr = ps.getWhere();
    	expr.accept(new ExpressionVisitorAdapter() {

            @Override
            protected void visitBinaryExpression(BinaryExpression expr) {
                if (expr instanceof ComparisonOperator) {
                	String value = expr.getRightExpression().toString();
                	value = value.replaceAll("'", "");
                	String condition = expr.getLeftExpression() + " " + expr.getStringExpression() + " " + value;
                	conditions.add(condition);
                   System.out.println("left=" + expr.getLeftExpression() + "  op=" +  expr.getStringExpression() + "  right=" + expr.getRightExpression());
                }
                super.visitBinaryExpression(expr); 
            }
            
            @Override
            public void visit(AndExpression expr) {
                expr.getLeftExpression().accept(this);
                expr.getRightExpression().accept(this);

            }
 //           @Override
//            public void visit(OrExpression expr) {
//            }
//            @Override
//            public void visit(Parenthesis parenthesis) {                
//            }
            
        });
    	
    	return conditions.toArray(new String[conditions.size()]);
    	
    }
    
    // Test
    public String toString(){
    	    	    	
    	Gson gson = new Gson();
    	JsonObject structure = new JsonObject();
    	structure.addProperty("Table", index);
    	structure.addProperty("Columns", gson.toJson(columns));
    	structure.addProperty("Conditions", gson.toJson(conditions));
    	// Add more attributes  	
    	
    	return structure.toString();
    }
    
}
