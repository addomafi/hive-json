Hive JSON Schema Finder
===

This project is a rough prototype that I've written to analyze large
collections of JSON documents and discover their Apache Hive
schema. I've used it to anaylyze the githubarchive.org's log data.

To build the project, use Maven (3.0.x) from http://maven.apache.org/.

Building the jar:

% mvn package

Run the program:

% bin/find-json-schema *.json.gz

I've uploaded the discovered schema for githubarchive.org to
https://gist.github.com/omalley/5125691.

#### Command to run schema discovery skipping from entities card_holder_name,device_free_pass
hadoop jar hive-json-1.0-SNAPSHOT-jar-with-dependencies.jar org.apache.hadoop.hive.json.DataSchemaDiscovery /tmp/data/schema card_holder_name,device_free_pass /tmp/data/raw/site=mco /tmp/data/raw/site=mla /tmp/data/raw/site=mlb /tmp/data/raw/site=mlc /tmp/data/raw/site=mlm /tmp/data/raw/site=mlu /tmp/data/raw/site=mlv /tmp/data/raw/site=mpe

#### Command to run schema discovery skipping from ALL entities just considering Snapshots
hadoop jar hive-json-1.0-SNAPSHOT-jar-with-dependencies.jar org.apache.hadoop.hive.json.DataSchemaDiscovery /tmp/data/schema ALL /tmp/data/raw/site=mco /tmp/data/raw/site=mla /tmp/data/raw/site=mlb /tmp/data/raw/site=mlc /tmp/data/raw/site=mlm /tmp/data/raw/site=mlu /tmp/data/raw/site=mlv /tmp/data/raw/site=mpe