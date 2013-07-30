import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.DoubleSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;


public class Utility {
	/**
	 * static method to execute range slice query, the limit on columns is set in the initial query
	 * the latter piece - column iterator simply iterates over that limit and returns the results
	 * @param ks
	 * @param columnFamily
	 * @param startKey
	 * @param endKey
	 * @param limit
	 * @return
	 */
	public static Rows<?,?,?> getCompositeRangeRowIterator(Keyspace ks, String columnFamily, String startKey, String endKey, 
																Composite startCol, Composite endCol, int limit)   {		        
		// first construct the slice range query
		RangeSlicesQuery<String, Composite, Double> rangeSlicesQuery =
	            HFactory.createRangeSlicesQuery(ks, StringSerializer.get(), new CompositeSerializer(), DoubleSerializer.get());
		rangeSlicesQuery.setColumnFamily(columnFamily);      
        rangeSlicesQuery.setKeys(startKey,endKey);
        rangeSlicesQuery.setRange(startCol, endCol, false, limit);
        // now execute and get results
        QueryResult<OrderedRows<String, Composite, Double>> results = rangeSlicesQuery.execute();
        // now get the multiple rows from this result set
        Rows<?, ?, ?> rows = (Rows<?, ?, ?>) results.get();
        return rows;
	}
	public static QueryResult<ColumnSlice<Composite, String>> getLimitSliceQuery
					(Keyspace ks, String columnFamily, String key, Composite startCol, Composite endCol, int limit){
		SliceQuery<String, Composite, String> sliceQuery =
    			HFactory.createSliceQuery(ks, StringSerializer.get(), new CompositeSerializer(), StringSerializer.get());
    	sliceQuery.setColumnFamily(columnFamily);
    	sliceQuery.setKey(key);
    	sliceQuery.setRange(startCol, endCol, false, limit);
    	QueryResult<ColumnSlice<Composite, String>> results = sliceQuery.execute();
    	return results;
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
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static Serializer<?> getSerializer(int i, List<String> typeList) {
		@SuppressWarnings({ })
		Class<AbstractSerializer> serialzerClass = null;
		try {
			serialzerClass = (Class<AbstractSerializer>) Class.forName(typeList.get(i));
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Method m = null;
		try {
			m = serialzerClass.getMethod("get");
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Object returnSerializer = null;
		try {
			returnSerializer = m.invoke(null, new Object[0]);
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return (Serializer<?>) returnSerializer;		
	}
}