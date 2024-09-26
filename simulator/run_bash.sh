#!/bin/bash

# Run the first instance of run.sh with nohup
nohup ./run.sh config/dasprotocol_K1_10.cfg > logs/dasprotocolSeedingStrategy1-k1-10.log &
# Wait for it to complete
wait

# Run the second instance of run.sh with nohup
nohup ./run.sh config/dasprotocol_K1_20.cfg > logs/dasprotocolSeedingStrategy1-k1-20.log &
# Wait for it to complete
wait

# Run the third instance of run.sh with nohup
nohup ./run.sh config/dasprotocol_K1_30.cfg > logs/dasprotocolSeedingStrategy1-k1-30.log &
# Wait for it to complete
wait

# Run the fourth instance of run.sh with nohup
nohup ./run.sh config/dasprotocol_K1_40.cfg > logs/dasprotocolSeedingStrategy1-k1-40.log &
# Wait for it to complete
wait

# Run the fifth instance of run.sh with nohup
nohup ./run.sh config/dasprotocol_K1_50.cfg > logs/dasprotocolSeedingStrategy1-k1-50.log &
# Wait for it to complete
wait

echo "All 5 instances of run.sh have completed."

