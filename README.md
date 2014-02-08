# Directions for running this test KijiMR / Cassandra job.

## Setting up your classpath

This is super-annoying.  I wrote a script to take care of this:
https://github.com/wibiclint/build-scripts/blob/master/bento_classpath.py.  Run the following
commands:

    python2.7 ${scripts}/bento_classpath.py
    source SOURCE_ME.sh
    export KIJI_CLASSPATH=target/wordcount-1.0-SNAPSHOT.jar:$KIJI_CLASSPATH

The `bento_classpath.py` script does the following:

- Creates a file `SOURCE_ME.sh` that sets `KIJI_CLASSPATH` equal to all of the non-Kiji JAR files
  on the build classpath Maven uses for the project
- Creates a directory `mylib` and puts in `mylib` symlinks to all of the JARs listed in
  `SOURCE_ME.sh`.  When you then specify `--lib=mylib` on the command line for your gatherer (see
  below), those JARs will be available to user code on the cluster.

## Basic setup

- Replace the bento JAR files for MR and Schema with the local builds.  The MR JAR is in
  `lib/distribution/hadoop2`.
- Start the BentoBox
- Start Cassandra
- Set up the Cassandra KijiURI: `export KIJI=kiji-cassandra://localhost:2181/localhost/9042/cas`
- Install the Kiji instance: `kiji install --kiji=${KIJI}`

## Insert some data

Create the table:

     ~/work/kiji/kiji-bento-dashi/schema-shell/bin/kiji-schema-shell \
        --kiji=${KIJI} \
        --file=src/main/layout/simple_table_desc.ddl


Insert data:

    kiji jar target/wordcount-1.0-SNAPSHOT.jar org.kiji.mr.setup.AddEntry $KIJI


Run the gatherer:

    kiji gather --gatherer=org.kiji.mr.gather.WordCountGatherer \
        --reducer=org.kiji.mr.reduce.WordCountReducer \
        --input="format=kiji table=${KIJI}/users" \
        --output="format=text file=output.txt_file nsplits=2" \
        --lib=mylib

