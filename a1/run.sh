#!/bin/sh
ant
for i in {1..2}
do
    ssh -X ecelinux$i 'cd ~/ece454/a1 && java -cp "lib/*:build/:src/" a1.StorageNode a1.config '$i
done
