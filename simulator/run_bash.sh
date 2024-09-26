#!/bin/bash

# Run the first instance of run.sh with nohup
nohup ./run.sh config/dasprotocol_K1.cfg > logs/dasprotocolSeedingStrategy1-k1.log &
# Wait for it to complete
wait

# Run the second instance of run.sh with nohup
nohup ./run.sh config/dasprotocol_K2.cfg > logs/dasprotocolSeedingStrategy1-k2.log &
# Wait for it to complete
wait

# Run the third instance of run.sh with nohup
nohup ./run.sh config/dasprotocol_K3.cfg > logs/dasprotocolSeedingStrategy1-k3.log &
# Wait for it to complete
wait

# Run the fourth instance of run.sh with nohup
nohup ./run.sh config/dasprotocol_K4.cfg > logs/dasprotocolSeedingStrategy1-k4.log &
# Wait for it to complete
wait

# Run the fifth instance of run.sh with nohup
nohup ./run.sh config/dasprotocol_K5.cfg > logs/dasprotocolSeedingStrategy1-k5.log &
# Wait for it to complete
wait

echo "All 5 instances of run.sh have completed."

