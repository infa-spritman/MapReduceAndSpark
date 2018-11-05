Hadoop MapReduce Twitter
Fall 2018

Code author
-----------
Sushant Pritmani

Installation
------------
These components are installed:
- JDK 1.8
- Hadoop 2.9.1
- Maven
- AWS CLI (for EMR execution)

Environment
-----------
1) Example ~/.bash_aliases:
export JAVA_HOME=/usr/lib/jvm/java-8-oracle
export HADOOP_HOME=/usr/local/hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

2) Explicitly set JAVA_HOME in $HADOOP_HOME/etc/hadoop/hadoop-env.sh:
export JAVA_HOME=/usr/lib/jvm/java-8-oracle

Execution
---------
All of the build & execution commands are organized in the Makefile.
1) Unzip project file.
2) Create a folder named "Twitter-dataset" inside given "input" folder.
3) Create a folder "nodes" inside "Twitter-dataset" folder and copy "nodes.csv" in it.
4) Create a folder "edges" inside "Twitter-dataset" folder and copy "edges.csv" in it.
   Create a folder  "followers_count" inside "Twitter-dataset" folder and run Followers count with nodes and edges
   input to get the followers count data.
5) Open command prompt.
6) Navigate to directory where project files unzipped.
7) Edit the Makefile to customize the environment at the top.
	Sufficient for standalone: hadoop.root, jar.name, local.input.nodes, local.input.edges
	Other defaults acceptable for running standalone.
8) Standalone Hadoop:
	make switch-standalone		-- set standalone Hadoop environment (execute once)
	make local
9) Pseudo-Distributed Hadoop: (https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation)
	make switch-pseudo			-- set pseudo-clustered Hadoop environment (execute once)
	make pseudo					-- first execution
	make pseudoq				-- later executions since namenode and datanode already running 
10) AWS EMR Hadoop: (you must configure the aws.emr.* config parameters at top of Makefile)
	make upload-input-aws		-- only before first execution
	make aws					-- check for successful execution with web interface (aws.amazon.com)
	download-output-aws			-- after successful execution & termination

Note: If there is problem in connecting to AWS, try adding subnet according to your region.

