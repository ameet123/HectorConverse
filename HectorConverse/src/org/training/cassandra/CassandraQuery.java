package org.training.cassandra;

import java.util.ArrayList;
import java.util.List;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;


public class CassandraQuery {
	private Cluster tutorialCluster;
	private final String CLUSTER_NAME = "artCluster";
	private Keyspace tutorialKeyspace;
	private ConfigurableConsistencyLevel READ_CONSISTENCY;
	private List<String> serialList = new ArrayList<String>();
//	private String columnFamily;
	private List<Object> componentElementList = new ArrayList<Object>();

	/**
	 * constructor sets up instances of cluster/keyspace, sets consistency level
	 * @param clusterAddress
	 * @param columnFamily
	 * @param keyspace
	 */
	public CassandraQuery(String clusterAddress, String columnFamily, String keyspace){
		// get a keyspace
		tutorialCluster = HFactory.getOrCreateCluster(CLUSTER_NAME,clusterAddress);
		tutorialKeyspace = HFactory.createKeyspace(keyspace, tutorialCluster, READ_CONSISTENCY);
		
		// set consistency
		READ_CONSISTENCY = new ConfigurableConsistencyLevel();
		READ_CONSISTENCY.setDefaultReadConsistencyLevel(HConsistencyLevel.ONE);
	}
	public void setColumnFamily(String CF){
//		this.columnFamily = CF;
		setSerializerList();
	}
	
	private void setSerializerList(){
		serialList.add("me.prettyprint.cassandra.serializers.StringSerializer");
		serialList.add("me.prettyprint.cassandra.serializers.StringSerializer");
		serialList.add("me.prettyprint.cassandra.serializers.DoubleSerializer");
		serialList.add("me.prettyprint.cassandra.serializers.DoubleSerializer");
	}
	
//	public void doIteratorQuery(Composite startCol, Composite endCol, String key){
//		CompositeSliceQueryIterator iter = new CompositeSliceQueryIterator(tutorialKeyspace, columnFamily, key, startCol, endCol);
//		
//	}
	/** 
	 * get a query iterator and iterate through the columns returned and print 
	 * desired components of the composite column 
	 * @param start
	 * @param end
	 */
	public void printColumnIterator(String CF, Composite start, Composite end) {
		CompositeSliceQueryIterator iter = new CompositeSliceQueryIterator(tutorialKeyspace, CF, "7955074002|2013-05|Electric usage", start, end);
	    int count = 0;
	    for ( HColumn<Composite, String> column : iter ) {
	    	System.out.printf("starttime: %s  endtime: %s  cost: %f Value= %f \n", 
		        column.getName().get(0,Utility.getSerializer(0,serialList)),
		        column.getName().get(1,Utility.getSerializer(1,serialList)),
		        column.getName().get(2,Utility.getSerializer(2,serialList)),
		        column.getValue()
	        );
	    	count++;
	    }
	    System.out.printf("Found %d columns\n",count);
	}
	
	@SuppressWarnings("unused")
	private List<Object> buildCompositeColumnElementList(HColumn<Composite,?> column){
		// get the composite column
		Composite columnName = column.getName();
		// loop through and create a list
		for (int i=0; i<columnName.size();i++){
			componentElementList.add(i, columnName.get(i, Utility.getSerializer(i, serialList)));
		}
		return componentElementList;
	}
}
