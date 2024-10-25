#!/bin/bash

./run.sh config/dasprotocol_Bstrat2_Vstrat1.cfg > logs/dasprotocol_Bstrat2_Vstrat1.log
# Wait for it to complete
wait

./run.sh config/dasprotocol_Bstrat2_Vstrat2.cfg > logs/dasprotocol_Bstrat2_Vstrat2.log
# Wait for it to complete
wait

./run.sh config/dasprotocol_Bstrat2_Vstrat3.cfg > logs/dasprotocol_Bstrat2_Vstrat3.log
# Wait for it to complet
wait

echo "All 3 instances of run.sh have completed."

