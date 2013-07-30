import java.util.Iterator;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.SliceQuery;

/**
 * inner class to return query iterator with automatic paging feature
 * @author ac2211
 */
public class CompositeSliceQueryIterator implements Iterable<HColumn<Composite,String>> {
	/**
	 * stores the slice iterator object
	 */
    private final ColumnSliceIterator<String,Composite,String> sliceIterator;
    /**
     * constructor based on a key and start/end objects
     * @param CF	Column Family
     * @param key
     * @param start
     * @param end
     */
    public CompositeSliceQueryIterator(Keyspace ks, String CF, String key, Composite start, Composite end) {
    	
    	SliceQuery<String, Composite, String> sliceQuery =
    			HFactory.createSliceQuery(ks, StringSerializer.get(), new CompositeSerializer(), StringSerializer.get());
    	sliceQuery.setColumnFamily(CF);
    	sliceQuery.setKey(key);
    	sliceIterator = new ColumnSliceIterator<String, Composite, String>(sliceQuery, start, end, false);
    }
    public CompositeSliceQueryIterator(Keyspace ks, String CF, String key, Composite start, Composite end, int limit) {
    	
    	SliceQuery<String, Composite, String> sliceQuery =
    			HFactory.createSliceQuery(ks, StringSerializer.get(), new CompositeSerializer(), StringSerializer.get());
    	sliceQuery.setColumnFamily(CF);
    	sliceQuery.setKey(key);
    	sliceQuery.setRange(start, end, false, limit);
    	sliceIterator = new ColumnSliceIterator<String, Composite, String>(sliceQuery, start, end, false);
    }
    /**
     * returns an iterator for columns of query
     */
	@Override
	public Iterator<HColumn<Composite, String>> iterator() {
		return sliceIterator;
	}
}