package org.training.cassandra;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.*;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
/**
 * Demonstrate various modes of cassandra query using Hector
 * @author ameet
 *
 */
public class CassandraQueriesViaHector {
	protected static Cluster tutorialCluster;
    protected static Keyspace tutorialKeyspace;
    String myKey = "elizabeth:lydia";
    String columnFamily = "chat_conversation_comp";
    Composite startCol;
    Composite endCol;
    /**
     * a List of serializers for the chat conversation table.
     * The table has one type of dynamic column which is a composite
     * the elements of composite are: TimeUUID:Talker
     * This list just lists them so we can get them conveniently
     */
    List<String> serialList = new ArrayList<String>() {
		private static final long serialVersionUID = -1607044520560409145L;
	{
		add("me.prettyprint.cassandra.serializers.UUIDSerializer");
		add("me.prettyprint.cassandra.serializers.StringSerializer");
	}};

	/**
	 * @param args
	 */
	public static void main(String[] args)  {
		CassandraQueriesViaHector ts = new CassandraQueriesViaHector();
		/**
		 * get a cluster handle by specifying the IP:Port and a name, which can be anything
		 */
		tutorialCluster = HFactory.getOrCreateCluster("artCluster","localhost:9160");
		/**
		 * define a consistency level for cassandra transactions
		 */
        ConfigurableConsistencyLevel ccl = new ConfigurableConsistencyLevel();
        ccl.setDefaultReadConsistencyLevel(HConsistencyLevel.ONE);
        /**
         * get a keyspace object based on the name of the keyspace
         */
        tutorialKeyspace = HFactory.createKeyspace("training_ks", tutorialCluster, ccl);
        // composite query     
            
        ts.startCol = Utility.UUIDfromDateString("2013-07-30 20:09:30", Composite.ComponentEquality.EQUAL);
        ts.endCol = Utility.UUIDfromDateString("2013-07-30 20:09:40", Composite.ComponentEquality.EQUAL);           
        
        /**
         * 1. how to get all keys, probably expensive on large # of keys, so need to be careful
         */
//        ts.getAllKeys();
        /**
         * 2. a top-n type of query, since the data is already ordered
         */
//        ts.topnQuery(3, "elizabeth:lydia");
        /**
//         * 3. range query which allows for a limit on columns returned and range of predicate values
         */
        ts.rangeQueryWithLimits(5, "2013-07-30 20:09:30", "2013-07-30 20:09:40", Composite.ComponentEquality.LESS_THAN_EQUAL);
        /**
         * 4. Slice Query with an iterator,range bounded by start->end
         */
        ts.getColumnSliceForKey(ts.columnFamily, ts.startCol,ts.endCol, ts.myKey);     
	}
	/**
	 * iterate using a RangeSliceQuery and get all keys.
	 * The important point here is that we don't really want to iterate over tens/hundreds/thousands or whatever # of columns
	 * that each key may have, since we only want row keys and not columns
	 */
	public void getAllKeys(){
		int rowCnt = 0;
		for ( Row<?, ?, ?> r: Utility.getCompositeRangeRowIterator(tutorialKeyspace, columnFamily, null, null, null, null, 2)){
			System.out.println("Rowkey:"+ r.getKey());
			rowCnt++;
		}
		System.out.println("Total row keys:"+rowCnt);
	}

	/**
	 * A Top-n type of query which essentially is looping over columns
	 * this works because we have already prepared our data model in such a way that
	 * the data is ordered according to our needs
	 * Hence it's more of a data modeling exercise rather than cassandra query, which is quite simple
	 * @param limit
	 * @param key
	 */
	public void topnQuery(int limit, String key) {
		QueryResult<ColumnSlice<Composite, String>> qr = Utility.getLimitSliceQuery(tutorialKeyspace, columnFamily, key, null, null, limit);
		/**
		 * we need the iter.hasNext() because without it the iteration is not kick-started
		 */
		for ( Iterator<HColumn<Composite, String>> iter = qr.get().getColumns().iterator(); iter.hasNext();) {
			HColumn<Composite, String> hc = iter.next();
			/**
			 * the 0th or 1st element in composite column is the Time element based on UUID Serializer
			 */
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
	 * get a query iterator and iterate through the columns returned and print 
	 * desired components of the composite column 
	 * this is for a specific key and based on Slice Query Operator
	 * @param start
	 * @param end
	 */
	public void getColumnSliceForKey(String CF, Composite start, Composite end, String key) {
		CompositeSliceQueryIterator iter = new CompositeSliceQueryIterator(tutorialKeyspace, CF, key, start, end);
	    int count = 0;
	    for ( HColumn<Composite, String> column : iter ) {
	    	UUID u = (UUID) column.getName().get(0,Utility.getSerializer(0,serialList));
	    	long time = TimeUUIDUtils.getTimeFromUUID(u);
	    	System.out.printf("Messagetime: %s  Talker:%s  Message= %s \n", 
		        new Date(time),
		        column.getName().get(1,Utility.getSerializer(1,serialList)),
		        column.getValue()
	        );
	    	count++;
	    }
	    System.out.printf("Found %d columns\n",count);
	}

	/**
	 * uses rangeslice query for multiple keys and get any columns with a # element
	 * Start Equality is always EQUAL
	 * @param limit
	 * @param startTime
	 * @param endTime
	 * @param endEquality
	 */
	public void rangeQueryWithLimits(int limit, String startTime, String endTime, ComponentEquality endEquality) {
		Composite startCol = Utility.UUIDfromDateString("2013-07-30 20:09:30", Composite.ComponentEquality.EQUAL);
		Composite endCol = Utility.UUIDfromDateString("2013-07-30 20:09:40", endEquality);
		int cnt = 0;
		int colCount = 0;
		for ( Row<?, ?, ?> r: Utility.getCompositeRangeRowIterator(tutorialKeyspace, columnFamily, this.myKey, this.myKey, startCol, endCol, limit)){
			System.out.println("Rowkey:"+ r.getKey());
			colCount = 0;
			// get column iterator
			CompositeColumnSliceIterator colIterator = new CompositeColumnSliceIterator(r);
			for ( HColumn<Composite,String> column : colIterator) {		
				UUID timeUuid =   (UUID) column.getName().get(0,Utility.getSerializer(0,serialList)) ;
				long time = TimeUUIDUtils.getTimeFromUUID(timeUuid);
				Date d = new Date(time);
				DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
				String dString = formatter.format(d);
			    System.out.printf("Messagetime: %s  Talker:%s  Message= %s\n", 
				        dString,			  
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