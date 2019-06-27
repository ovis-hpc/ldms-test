#!/bin/bash

#SBATCH -N 2
#SBATCH -D /db

srun bash /db/prog.sh
