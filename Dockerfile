FROM motus/tomcatbase:tomcat7-master-10

ADD turbine-web/build/libs/turbine-web-1.0.0-SNAPSHOT.war /usr/local/apache-tomcat/webapps/turbine.war