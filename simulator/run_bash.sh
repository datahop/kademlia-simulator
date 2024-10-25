#!/bin/bash

# Run the first instance of run.sh with nohup
./run.sh config/dasprotocol_Bstrat0_Vstrat1.cfg > logs/dasprotocol_Bstrat0_Vstrat1.log
# Wait for it to complete
wait

./run.sh config/dasprotocol_Bstrat0_Vstrat2.cfg > logs/dasprotocol_Bstrat0_Vstrat2.log
# Wait for it to complete
wait

./run.sh config/dasprotocol_Bstrat0_Vstrat3.cfg > logs/dasprotocol_Bstrat0_Vstrat3.log
# Wait for it to complete
wait


echo "All 3 instances of run.sh have completed."

