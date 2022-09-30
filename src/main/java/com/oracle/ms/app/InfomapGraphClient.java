package com.oracle.ms.app;


import java.util.Set;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Supplier;

import oracle.pg.rdbms.AdbGraphClient;
import oracle.pg.rdbms.AdbGraphClientConfiguration;
import oracle.pgx.api.PgxGraph;
import oracle.pgx.api.Analyst;
import oracle.pgx.api.EdgeProperty;
import oracle.pgx.api.Partition;
import oracle.pgx.api.PgxSession;
import oracle.pgx.api.PgxVertex;
import oracle.pgx.api.ServerInstance;
import oracle.pgx.api.VertexCollection;
import oracle.pgx.api.VertexProperty;
import oracle.pgx.common.types.PropertyType;
import oracle.pgx.config.GraphConfig;
import oracle.pgx.config.GraphConfigBuilder;

public class InfomapGraphClient {

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		
		var config = AdbGraphClientConfiguration.builder();
		config.tenant("ocid1.tenancy.oc1..aaaaaaaabls4dzottlktt774tu3knax6crpozycjhrqpm73thfryxwlkmkba");
		config.database("medicalrecordsdb");
		config.username("ADMIN");
		config.password("Welcome12345");
		config.endpoint("https://bsenjiat5lmurtq-medicalrecordsdb.adb.us-ashburn-1.oraclecloudapps.com/");
		
		var client = new AdbGraphClient(config.build());
		
		if (!client.isAttached()) {
			var job = client.startEnvironment(10);
			job.get();
			System.out.println("job details: name=" + job.getName() + "type= " + job.getType() + "created_by= " + job.getCreatedBy());
		}

		ServerInstance instance = client.getPgxInstance();
		PgxSession session = instance.createSession("MedicalRecAdbGraphSessionName");
		//MED_REC_PG_OBJ_G
		//MED_RECS_NEW_G - 259 nodes
		//PgxGraph graph = session.readGraphByName("MED_RECS_NEW_G", oracle.pgx.api.GraphSource.PG_VIEW);
		//PgxGraph graph = session.getGraph("MED_REC_PG_OBJ_G");
		
		/*
		 * Supplier<GraphConfig> pgxConfig = () -> { return
		 * GraphConfigBuilder.forPropertyGraphRdbms() .setName("hr")
		 * 
		 * .setLoadVertexLabels(true) .setLoadEdgeLabel(true) .build(); };
		 */
		
		/*
		 * GraphConfig graphConfig = AdbGraphClientConfiguration.builder(); graph =
		 * session.readGraphWithProperties(config);
		 */
		
		GraphConfig graphConfig = 
				GraphConfigBuilder.forPropertyGraphRdbms()
				.setName("MED_REC_PG_OBJ_G")
				.addVertexProperty("TABLE_NAME", PropertyType.STRING)
				.addEdgeProperty("TABLE1", PropertyType.STRING)
				.addEdgeProperty("TABLE2", PropertyType.STRING)
				.addEdgeProperty("TOTAL_AFFINITY", PropertyType.DOUBLE)
		
		// .setPartitionWhileLoading(PartitionWhileLoading.BY_LABEL)
		 .setLoadVertexLabels(true)
		 .setLoadEdgeLabel(true)
		 .build();
		PgxGraph graph = session.readGraphWithProperties(graphConfig);
		
		System.out.println("Graph : " + graph);
		Analyst analyst = session.createAnalyst();
		EdgeProperty<Double> weight = graph.getEdgeProperty("TOTAL_AFFINITY");
		try {
			VertexProperty<Integer, Double> rank = analyst.weightedPagerank(graph, 1e-16, 0.85, 1000, true, weight);
			VertexProperty<Integer, Long> module = graph.createVertexProperty(PropertyType.LONG,"Community");
			Partition<Integer> promise = analyst.communitiesInfomap(graph, rank, weight, 0.15, 0.0001, 1, module);
			Set<VertexProperty<?, ?>> vProps = graph.getVertexProperties();
			for (VertexProperty<?,?> prop : vProps) {
				System.out.println("Prop : " + prop.getName());
			}
			graph.queryPgql("SELECT n.Community,n.TABLE_NAME FROM MATCH (n) order by n.Community").print().close();
			
			/*VertexCollection<Integer> first_component = promise.getPartitionByIndex(0);

			System.out.println("first_component : " + first_component);
			
			for (VertexCollection<Integer> partition : promise){
			    System.out.println("======== ===== =====partition : " + partition);
			    for (PgxVertex<Integer> vertexInCommunity : partition){
			    	System.out.println("Res : " + vertexInCommunity.getProperty("TABLE_NAME"));
			    }
			}*/
		} catch (ExecutionException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		session.close();
		
	}
}
