package com.motus.infra.turbine.discovery;


import java.io.InputStream;
import java.net.URL;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.configuration.EnvironmentConfiguration;
import org.apache.commons.io.IOUtils;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import com.netflix.config.ConcurrentCompositeConfiguration;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;
import com.netflix.turbine.discovery.Instance;
import com.netflix.turbine.discovery.InstanceDiscovery;

/**
 * Implements instance discovery by looking through marathon 
 * 
 * @author srankin
 *
 */
public class MarathonInstanceDiscovery implements InstanceDiscovery {

    public Collection<Instance> getInstanceList() throws Exception {
        
        Set<Instance> sets = new HashSet<>();
        
        // Add environment variables to the config factory
        DynamicPropertyFactory configFactory = DynamicPropertyFactory.getInstance();
        @SuppressWarnings("static-access")
        ConcurrentCompositeConfiguration compositeConfigSource = (ConcurrentCompositeConfiguration)configFactory.getBackingConfigurationSource();
        compositeConfigSource.addConfiguration(new EnvironmentConfiguration());
        
        // Get the currently configured marathon URL
        DynamicStringProperty marathonUrlProperty = configFactory.getStringProperty("turbine.marathon.url", null);
        String marathonUrlString = marathonUrlProperty.get();
        
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
                System.out.println("Found app with name " + appId);
                
                // Get the labels
                JSONObject labels = appObject.getJSONObject("labels");
                
                // See if it has a hystrix label and if so, if it's true
                if (labels.containsKey("turbineUrl")) {
                    System.out.println("App " + appId + " has turbine label, getting tasks");
                    sets.addAll(getInstancesForApp(marathonUrlString + appId));
                }
            }
        }
       
        return sets;
    }

    private Set<Instance> getInstancesForApp(String appUrl) throws Exception {
        Set<Instance> instances = new HashSet<Instance>();
        URL marathonAppUrl = new URL(appUrl);
        try (InputStream is = marathonAppUrl.openStream()) {
            String contents = IOUtils.toString(is);
            
            JSONObject body = JSONObject.fromObject(contents);
            JSONObject app = body.getJSONObject("app");
            
            // The cluster name is the app ID
            String cluster = app.getString("id").replaceAll("^/", "").replace('/', '_');
            
            // Iterate over the tasks and create instances for the healthy tasks
            JSONArray taskArray = app.getJSONArray("tasks");
            @SuppressWarnings("unchecked")
            Iterator<JSONObject> taskIter = taskArray.iterator();
            while (taskIter.hasNext()) {
                JSONObject task = taskIter.next();
                
                // Make sure it has at least one healthy task 
                if (task.containsKey("healthCheckResults")) {
                    JSONArray healthChecks = task.getJSONArray("healthCheckResults");
                    if (healthChecks.size() > 0) {
                        JSONObject firstHealthCheck = healthChecks.getJSONObject(0);
                        if (firstHealthCheck.getBoolean("alive")) {
                            Instance newInstance = new Instance(task.getString("host") + ":" + task.getJSONArray("ports").getInt(0),
                                                                cluster,
                                                                true);
                            instances.add(newInstance);
                            
                            System.out.println("Adding instance " + newInstance.toString());
                        }
                    }
                }
            }
        }
        
        return instances;
    }
}
