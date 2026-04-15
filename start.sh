#!/bin/bash
mvn -q package -DskipTests
java -jar target/lsm-store-1.0-SNAPSHOT.jar
