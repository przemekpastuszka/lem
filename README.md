# LEM project
Active development years: 2013

## Summary
For now: simple tests to verify, that 'append' and 'pluggable block placement policy' hadoop's features work.
Expect more interesting stuff here later. :)

## Working with source code
You will need:
* Java 6 JDK: www.oracle.com/technetwork/java/javase/downloads/index.html
* gradle 1.6 (or newer) build tool: www.gradle.org/
* fabric: http://docs.fabfile.org/en/1.7/
* (optional - if you want to setup the cluster) whirr: http://whirr.apache.org/

### Compilation
Just do ```gradle build``` in root directory. gradle will download all required dependencies and compile classes

### Editing
I strongly recommend IntelliJ for editing the source. The community edition is free for non-commercial purposes and can be downloaded here: www.jetbrains.com/idea/
Execute ```gradle idea```, which will generate all required files. Then open the project in IntelliJ and enjoy!

## Running the features tests
* using in-memory hadoop cluster - ``gradle runInMemory``
* using real hadoop cluster (you need to set up cluster yourself) - ``gradle runOnHadoop -Dmaster=masterAddress``, where ``masterAddress`` is an external address of the master machine in your cluster (can be ``localhost`` too)

