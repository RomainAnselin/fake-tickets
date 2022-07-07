#!/bin/bash

# incval=$(( ${startval} + ${numrec} ))
numrec=9
for startval in {1000000..2000000..10}; do
  printf "python insert-astra.py ${startval} ${numrec}\n"
  python insert-astra.py ${startval} ${numrec}
# Launch script
done
