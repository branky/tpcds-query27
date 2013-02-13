

VERSION=2-pristine
YARN_HOME=$(shell ls -d /home/gopal/hw/hadoop-$(VERSION)/hadoop-dist/target/hadoop-*-SNAPSHOT/)
HADOOP_HOME=$(shell ls -d /home/gopal/hw/hadoop-$(VERSION)/hadoop-dist/target/hadoop-*-SNAPSHOT/)
EXPORTS=export HADOOP_HOME=$(HADOOP_HOME); \
	export YARN_HOME=$(YARN_HOME); \
	true
RANDOM=$(shell /bin/bash -c "echo \$$RANDOM";)

all:
	$(EXPORTS); $(HADOOP_HOME)/bin/hadoop jar target/mapjoin-bench-1.0-SNAPSHOT.jar file:///tmp/hive/tpcds/ file:///tmp/q27.$(RANDOM)/

yarn:
	$(EXPORTS); $(HADOOP_HOME)/bin/hadoop --config . jar target/mapjoin-bench-1.0-SNAPSHOT.jar file:///tmp/hive/tpcds/ file:///tmp/q27.$(RANDOM)/ $(OPTS)

yourkit:
	$(EXPORTS); $(HADOOP_HOME)/bin/hadoop --config yourkit/ jar target/mapjoin-bench-1.0-SNAPSHOT.jar file:///tmp/hive/tpcds/ file:///tmp/q27.$(RANDOM)/ $(OPTS)

.PHONY: yourkit
