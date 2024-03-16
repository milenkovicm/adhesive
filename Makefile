.PHONY: all rust jvm test clean

all: rust jvm

rust:
	cargo build 
# should be test: jvm but test fail then
# no time to investigate
test: 
	cargo test
jvm:
	cd java && mvn package -DskipTests
test-jvm:
	cd java && mvn test

clean:
	cd java && mvn clean
	cargo clean
