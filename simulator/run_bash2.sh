#!/bin/bash

./run.sh config/dasprotocol_Bstrat1_Vstrat1.cfg > logs/dasprotocol_Bstrat1_Vstrat1.log
# Wait for it to complete
wait

./run.sh config/dasprotocol_Bstrat1_Vstrat2.cfg > logs/dasprotocol_Bstrat1_Vstrat2.log
# Wait for it to complete
wait

./run.sh config/dasprotocol_Bstrat1_Vstrat3.cfg > logs/dasprotocol_Bstrat1_Vstrat3.log
# Wait for it to complete
wait

echo "All 3 instances of run.sh have completed."

