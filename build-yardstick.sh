#!/usr/bin/env bash

set -x

mvn clean package -Prelease,benchmarks,yardstick,ml -Dgridgain.edition=fabric -Dmaven.javadoc.skip=true \
-DskipTests -U -pl modules/yardstick -am

mkdir modules/yardstick/target/yardstick-private

cp -r modules/yardstick/target/assembly/** modules/yardstick/target/yardstick-private
cp -r bin/control.sh modules/yardstick/target/yardstick-private/bin
cp -r bin/include modules/yardstick/target/yardstick-private/bin

tar cfz modules/yardstick/target/yardstick-private.tar.gz -C modules/yardstick/target/ yardstick-private

# copy yardstick-private.tar.gz to 172.25.1.33
# unpack to /storage/ssd/<name>/
# cd /storage/ssd/<name>/yardstick-private
# bin/benchmark-run-all.sh config/streamer/benchmark-streamer.properties
