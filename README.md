This is a java-based command line tool that deposits SIP packages to the ROSETTA digital archiving system.

The workflow is as follows

1) Create a ROSETTA SIP compliant folder that lives in Rosetta_deposit/dps-sdk-deposit/data/depositExamples 

An example of such a package exists in this directory. The credentials will have to be replaced with real ones.

2) Upload this folder (a separate process, not using this tool) to ROSETTA in the correct folder on that server

3) Update the settings.properties file in the SIP package with this folder name

4) Run this tool using "java -jar deposit.jar"

