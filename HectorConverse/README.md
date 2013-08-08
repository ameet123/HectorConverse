HectorConverse
==============

queries against cassandra using hector
<html>
  <head>
		<title></title>
	</head>
	<body>
		<p>
			HectorConverse:</p>
		<p>
			============</p>
		<p>
			This application demonstrates a few main types of queries against Cassandra.</p>
		<p>
			this application requires ConverseCass application from&nbsp;https://github.com/ameet123/ConverseCass&nbsp;</p>
		<p>
			It makes use of Hector client as opposed to Pelops in the ConverseCass.</p>
		<p>
			Reference Libraries:</p>
		<ul>
			<li>
				<em>hector-core-1.0.5-SNAPSHOT.jar </em>
				<ul>
					<li>
						from&nbsp;https://github.com/hector-client/hector/archive/master.zip, &nbsp;you may have to build it as per&nbsp;<a href="https://github.com/hector-client/hector">https://github.com/hector-client/hector</a></li>
				</ul>
			</li>
			<li>
				<em>slf4j-api-1.7.2.jar</em> - part of cassandra installation</li>
			<li>
				<em>libthrift-0.7.0.jar </em>- part of cassandra jar</li>
			<li>
				<em>guava-13.0.1.jar</em> - part of cassandra</li>
			<li>
				<em>apache-cassandra-thrift-1.2.3.jar </em>- part of cassandra</li>
			<li>
				<em>slf4j-log4j12-1.7.2.jar</em> - part of cassandra</li>
			<li>
				<em>commons-lang-2.6.jar</em> part of cassandra</li>
			<li>
				<em>log4j-1.2.16.jar</em> - part of cassandra</li>
			<li>
				<em>uuid-3.4.jar</em> -&nbsp;<a href="http://johannburkard.de/software/uuid/">http://johannburkard.de/software/uuid/</a></li>
		</ul>
		<p>
			There are 4 types of queries:</p>
		<ol>
			<li>
				how to get all keys, probably expensive on large # of keys, so need to be careful</li>
		</ol>
		<div style="margin-left: 80px;">
			<code>ts.getAllKeys();</code></div>
		<div>
			&nbsp; &nbsp; &nbsp;2. &nbsp;a top-n type of query, since the data is already ordered</div>
		<div style="margin-left: 80px;"><code>
			ts.topnQuery(3, &quot;elizabeth:lydia&quot;);</code></div>
		<div>
			&nbsp; &nbsp; &nbsp;3. &nbsp;range query which allows for a limit on columns returned and range of predicate values</div>
		<div style="margin-left: 80px;"><code>
			ts.rangeQueryWithLimits(5, &quot;2013-07-30 20:09:30&quot;, &quot;2013-07-30 20:09:40&quot;,
			Composite.ComponentEquality.LESS_THAN_EQUAL);</div>
		<div></code>
			&nbsp; &nbsp; &nbsp;4. &nbsp;&nbsp;Slice Query with an iterator,range bounded by start-&gt;end</div>
		<div style="margin-left: 80px;"><code>
			ts.getColumnSliceForKey(ts.columnFamily, ts.startCol,ts.endCol, ts.myKey);</code> &nbsp;&nbsp;</div>
		<div style="margin-left: 80px;">
			&nbsp;</div>
	</body>
</html>
