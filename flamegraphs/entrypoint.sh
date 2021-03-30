ls -a
java -jar flame-jfr/target/flame-graph-jfr-1.0-SNAPSHOT-shaded.jar -i "$NAME.jfr" -o "$NAME.fld"
flame-graph/flamegraph.pl "$NAME.fld" > /home/work/volume/"$NAME.svg"
ls volume