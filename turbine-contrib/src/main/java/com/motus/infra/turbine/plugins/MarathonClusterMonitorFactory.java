package com.motus.infra.turbine.plugins;

import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.configuration.EnvironmentConfiguration;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.config.ConcurrentCompositeConfiguration;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;
import com.netflix.turbine.data.AggDataFromCluster;
import com.netflix.turbine.discovery.Instance;
import com.netflix.turbine.handler.PerformanceCriteria;
import com.netflix.turbine.handler.TurbineDataHandler;
import com.netflix.turbine.monitor.TurbineDataMonitor;
import com.netflix.turbine.monitor.cluster.AggregateClusterMonitor;
import com.netflix.turbine.monitor.cluster.ClusterMonitor;
import com.netflix.turbine.monitor.cluster.ClusterMonitorFactory;

/**
 * Plugin for using the {@link AggregateClusterMonitor} on startup.  Reads from Marathon. 
 *
 * <p> Note that since the aggregator shuts down when no-one is listening we just attach a bogus no-op {@link TurbineDataHandler}
 * here to keep the aggergator running. This helps in getting the data really fast when a real listener comes along, since the
 * aggregator will then already have all the connections set up and data will be flowing from the multiple server instances.
 *
 */
public class MarathonClusterMonitorFactory implements ClusterMonitorFactory<AggDataFromCluster> {

    private static final Logger logger = LoggerFactory.getLogger(MarathonClusterMonitorFactory.class);

    /**
     * @return {@link ClusterMonitor}<{@link AggDataFromCluster}>
     */
    @Override
    public ClusterMonitor<AggDataFromCluster> getClusterMonitor(String name) {
        TurbineDataMonitor<AggDataFromCluster> clusterMonitor = AggregateClusterMonitor.AggregatorClusterMonitorConsole.findMonitor(name + "_agg");
        return (ClusterMonitor<AggDataFromCluster>) clusterMonitor;
    }

    /**
     * Inits all configured cluster monitors
     */
    @Override
    public void initClusterMonitors() {

        for(String clusterName : getClusterNames()) {
            logger.info("Starting monitor for cluster " + clusterName);
            ClusterMonitor<AggDataFromCluster> clusterMonitor = (ClusterMonitor<AggDataFromCluster>) AggregateClusterMonitor.findOrRegisterAggregateMonitor(clusterName);
            clusterMonitor.registerListenertoClusterMonitor(StaticListener);
            try {
                clusterMonitor.startMonitor();
            } catch (Exception e) {
                logger.warn("Could not init cluster monitor for: " + clusterName);
                clusterMonitor.stopMonitor();
                clusterMonitor.getDispatcher().stopDispatcher();
            }
        }
    }

    /**
     * shutdown all configured cluster monitors
     */
    @Override
    public void shutdownClusterMonitors() {

        for(String clusterName : getClusterNames()) {
            ClusterMonitor<AggDataFromCluster> clusterMonitor = (ClusterMonitor<AggDataFromCluster>) AggregateClusterMonitor.findOrRegisterAggregateMonitor(clusterName);
            clusterMonitor.stopMonitor();
            clusterMonitor.getDispatcher().stopDispatcher();
        }
    }

    private List<String> getClusterNames() {

        List<String> clusters = new ArrayList<String>();
        
        // Add environment variables to the config factory
        DynamicPropertyFactory configFactory = DynamicPropertyFactory.getInstance();
        @SuppressWarnings("static-access")
        ConcurrentCompositeConfiguration compositeConfigSource = (ConcurrentCompositeConfiguration)configFactory.getBackingConfigurationSource();
        compositeConfigSource.addConfiguration(new EnvironmentConfiguration());
        
        // Get the currently configured marathon URL
        DynamicStringProperty marathonUrlProperty = configFactory.getStringProperty("turbine.marathon.url", null);
        String marathonUrlString = marathonUrlProperty.get();
        
        try {
           
            // If the URL is null, then bail
            if (marathonUrlString == null) {
                throw new Exception("No configured marathon URL");
            }
            
            URL marathonUrl = new URL(marathonUrlString);
            
            // Get the list of running apps and iterate through them
            try (InputStream is = marathonUrl.openStream()) {
                
                String contents = IOUtils.toString(is);
                
                JSONObject body = JSONObject.fromObject(contents);
                JSONArray appArray = body.getJSONArray("apps");
                
                @SuppressWarnings("unchecked")
                Iterator<JSONObject> iter = appArray.iterator();
                while (iter.hasNext()) {
                    JSONObject appObject = iter.next();
                    
                    String appId = appObject.getString("id");
                    
                    // Get the labels
                    JSONObject labels = appObject.getJSONObject("labels");
                    
                    // See if it has a hystrix label and if so, if it's true
                    if (labels.containsKey("turbineUrl")) {
                        String cluster = appId.replaceAll("^/", "").replace('/', '_');
                        logger.info("Found app, adding cluster " + cluster);
                        clusters.add(cluster);
                        
                        // Add a property for the cluster suffix
                        logger.info("Setting property " + "turbine.instanceUrlSuffix." + cluster + " to " + labels.getString("turbineUrl"));
                        compositeConfigSource.setProperty("turbine.instanceUrlSuffix." + cluster, labels.getString("turbineUrl"));
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Could not get cluster information from marathon", e);
            throw new RuntimeException(e);
        }
        
        return clusters;
    }
    
    private TurbineDataHandler<AggDataFromCluster> StaticListener = new TurbineDataHandler<AggDataFromCluster>() {

        @Override
        public String getName() {
            return "StaticListener_For_Aggregator";
        }

        @Override
        public void handleData(Collection<AggDataFromCluster> stats) {
        }

        @Override
        public void handleHostLost(Instance host) {
        }

        @Override
        public PerformanceCriteria getCriteria() {
            return NonCriticalCriteria;
        }

    };

    private PerformanceCriteria NonCriticalCriteria = new PerformanceCriteria() {

        @Override
        public boolean isCritical() {
            return false;
        }

        @Override
        public int getMaxQueueSize() {
            return 0;
        }

        @Override
        public int numThreads() {
            return 0;
        }
    };
}
