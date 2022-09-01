# Kademlia Simulator

## Overview
 
This is a Kademlia Simulator that was used in the research project for the new Service Discovery in Ethereum 2.0 (Discv5) (available at: https://github.com/datahop/p2p-service-discovery). The simulator is built on top of [PeerSim](http://peersim.sourceforge.net/) and it is based on the Kademlia implementation from Daniele Furlan and Maurizio Bonani that can be found [here](http://peersim.sourceforge.net/code/kademlia.zip).

## Requirements

To run the simulator it is necessary to have Java and Maven installed. For Ubuntu systems just run:

```shell
$ sudo apt install maven default-jdk
```

## How to run it

To execut a simulation it is necessary to call the run.sh, with a configuration file as a parameter. For example:

```shell
$ ./run.sh config/kademlia.cfg
```

## How to create your own scenario file

Follow PeerSim [documentation](http://peersim.sourceforge.net/tutorialed/)

## Code Documentation

TBC 

## How to create a protocol on top of Kademlia

TBC
