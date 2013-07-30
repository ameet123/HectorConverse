import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.*;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;

public class TestHector {
	protected static Cluster tutorialCluster;
    protected static Keyspace tutorialKeyspace;
    String myKey = "7955074002|2013-05|Electric usage";
    String columnFamily = "chat_conversation_comp";
    Composite startCol;
    Composite endCol;
    List<String> serialList = new ArrayList<String>() {
		private static final long serialVersionUID = -1607044520560409145L;
	{
//		add("me.prettyprint.cassandra.serializers.TimeUUIDSerializer");
		add("me.prettyprint.cassandra.serializers.UUIDSerializer");
		add("me.prettyprint.cassandra.serializers.StringSerializer");
	}};

	/**
	 * @param args
	 */
	public static void main(String[] args)  {
		TestHector ts = new TestHector();
		
		tutorialCluster = HFactory.getOrCreateCluster("artCluster","localhost:9160");
        ConfigurableConsistencyLevel ccl = new ConfigurableConsistencyLevel();
        ccl.setDefaultReadConsistencyLevel(HConsistencyLevel.ONE);
        tutorialKeyspace = HFactory.createKeyspace("training_ks", tutorialCluster, ccl);
        System.out.println("Keyspace name:"+tutorialKeyspace.getKeyspaceName());
        // composite query
        ts.startCol = compositeFrom("2013-05-13 20:15", Composite.ComponentEquality.EQUAL);
        ts.endCol = compositeFrom("2013-05-13 23:15", Composite.ComponentEquality.LESS_THAN_EQUAL);
//        ts.endCol = null;

//        ts.printColumnsFor(ts.columnFamily, ts.startCol,ts.endCol);
        
//        ts.printRangeQueryResults3();
//        ts.getAllKeys();
        ts.topnQuery(3, "elizabeth:lydia");
	}
	public void getAllKeys(){
		int rowCnt = 0;
		for ( Row<?, ?, ?> r: Utility.getCompositeRangeRowIterator(tutorialKeyspace, columnFamily, null, null, null, null, 2)){
			System.out.println("Rowkey:"+ r.getKey());
			rowCnt++;
		}
		System.out.println("Total row keys:"+rowCnt);
	}

	public void topnQuery(int limit, String key) {
		QueryResult<ColumnSlice<Composite, String>> qr = Utility.getLimitSliceQuery(tutorialKeyspace, columnFamily, key, null, null, limit);
		for ( Iterator<HColumn<Composite, String>> iter = qr.get().getColumns().iterator(); iter.hasNext();) {
			HColumn<Composite, String> hc = iter.next();
			UUID timeUuid =   hc.getName().get(0,me.prettyprint.cassandra.serializers.UUIDSerializer.get()) ;
			long time = TimeUUIDUtils.getTimeFromUUID(timeUuid);
            System.out.printf("Time: %s talker:%s message:%s \n",             		
            		new Date(time),
            		hc.getName().get(1,StringSerializer.get()),
            		hc.getValue()
            );
          }
	}
	/**
	 * form composite column to be used for start and end
	 * @param equalityOp
	 * @return Composite
	 */
	public static Composite compositeFrom(String componentName, Composite.ComponentEquality equalityOp){
		Composite composite = new Composite();
		composite.addComponent(componentName, StringSerializer.get(), "UTF8Type", equalityOp);
		return composite;
		
	}
	/** 
	 * get a query iterator and iterate through the columns returned and print 
	 * desired components of the composite column 
	 * @param start
	 * @param end
	 */
	public void printColumnsFor(String CF, Composite start, Composite end) {
		CompositeSliceQueryIterator iter = new CompositeSliceQueryIterator(tutorialKeyspace, CF, "7955074002|2013-05|Electric usage", start, end);
	    int count = 0;
	    for ( HColumn<Composite, String> column : iter ) {
	    	System.out.printf("Messagetime: %s  Talker:%s  Message= %s \n", 
		        column.getName().get(0,Utility.getSerializer(0,serialList)),
		        column.getName().get(1,Utility.getSerializer(1,serialList)),
		        column.getValue()
	        );
	    	count++;
	    }
	    System.out.printf("Found %d columns\n",count);
	}

	public void printRangeQueryResults3() {
		int cnt = 0;
		int colCount = 0;
		for ( Row<?, ?, ?> r: Utility.getCompositeRangeRowIterator(tutorialKeyspace, columnFamily, this.myKey, this.myKey, startCol, endCol, 8)){
			System.out.println("Rowkey:"+ r.getKey());
			colCount = 0;
			// get column iterator
			CompositeColumnSliceIterator colIterator = new CompositeColumnSliceIterator(r);
			for ( HColumn<Composite,String> column : colIterator) {							
			    System.out.printf("Messagetime: %s  Talker:%s  Message= %s\n", 
				        column.getName().get(0,Utility.getSerializer(0,serialList)),
				        column.getName().get(1,Utility.getSerializer(1,serialList)),
				        column.getValue()
			    );
			    colCount++;
            }
			System.out.println("Total columns  = "+colCount);
			cnt++;
		}
		System.out.println("Total Row count:"+cnt);
	}
}