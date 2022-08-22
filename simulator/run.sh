if [ "$#" -ne 1 ]; then
    echo "Provide the config file name to run"
    exit 1
fi
mkdir -p logs
rm -f logs/*.csv
java -Xmx200000m -cp lib/djep-1.0.0.jar:lib/jep-2.3.0.jar:target/service-discovery-1.0-SNAPSHOT.jar:lib/gs-core-2.0.jar:lib/pherd-1.0.jar:lib/mbox2-1.0.jar:lib/gs-ui-swing-2.0.jar -ea peersim.Simulator $1
