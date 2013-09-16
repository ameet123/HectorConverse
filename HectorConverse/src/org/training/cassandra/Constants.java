package org.training.cassandra;

import java.util.ArrayList;
import java.util.List;

import me.prettyprint.hector.api.beans.Composite;

public class Constants {
	public static final String DATA_CF = "chat_conversation_comp";
	/**
     * a List of serializers for the chat conversation table.
     * The table has one type of dynamic column which is a composite
     * the elements of composite are: TimeUUID:Talker
     * This list just lists them so we can get them conveniently
     */
	public static List<String> SERIALIZER_LIST = new ArrayList<String>() {
		private static final long serialVersionUID = -1607044520560409145L;
	{
		add("me.prettyprint.cassandra.serializers.UUIDSerializer");
		add("me.prettyprint.cassandra.serializers.StringSerializer");
	}};
	public static String MYKEY = "elizabeth:lydia";
	public static String CLUSTERNAME = "trainingCluster";
	public static String CLUSTERID = "localhost:9160";
	public static String KEYSPACE = "training_ks";
	public static String start = "2013-09-08 17:00:00";
	public static String end = "2013-09-08 20:09:40";
	
	public static Composite startCol = Utility.UUIDfromDateString(start, Composite.ComponentEquality.EQUAL);
	public static Composite endCol = Utility.UUIDfromDateString(end, Composite.ComponentEquality.GREATER_THAN_EQUAL);
}