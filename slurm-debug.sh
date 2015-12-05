#!/bin/bash
####### MAKE SURE THAT NODES ARE EXCLUSIVE FOR YOU
#SBATCH --exclusive
#SBATCH --partition=debug
####### CUSTOMIZE THIS SECTION FOR YOUR JOB
####### KEEP --mem=64000 TO USE FULL MEMORY
#######SBATCH --partition=general-compute
#SBATCH --mem=20000
#SBATCH --job-name="2Nodes1TN4EC--4GDriver4GExecutor-200Kparts"
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=1
#SBATCH --output=%j4N1C4E5Kparts.stdout
#SBATCH --error=%j4N1C4E5Kparts.stderr
#SBATCH --mail-user=sarthakd@buffalo.edu
#SBATCH --mail-type=ALL
#SBATCH --time=01:00:00
#SBATCH --constraint=CPU-E5645
# --ntasks-per-node SETS NUMBER OF SPARK EXECUTORS
# executor_cores SETS NUMBER OF CORES PER EXECUTOR
executor_cores=4

# MAKE SURE THAT SPARK_LOG_DIR AND SPARK_WORKER_DIR
# ARE SET IN YOUR BASHRC, FOR EXAMPLE:
# export SPARK_LOG_DIR=/tmp
# export SPARK_WORKER_DIR=/scratch/your_login

# ADD EXTRA MODULES HERE IF NEEDED


# SET YOUR COMMAND AND ARGUMENTS
PROG="knn.jar"

ARGS="/user/sarthakd/200k.txt /user/sarthakd/knnOutput4N3TN4E 20000 10"



####### DO NOT EDIT THIS PART
module load java/1.8.0_45
module load hadoop/2.6.0
module load spark/1.4.1-hadoop

# GET LIST OF NODES
NODES=(`srun hostname | sort`)

NUM_NODES=${#NODES[@]}
LAST=$((NUM_NODES - 1))

# FIRST NODE IS MASTER
ssh ${NODES[0]} "cd $SPARK_HOME; ./sbin/start-master.sh"
MASTER="spark://${NODES[0]}:7077"

# ALL OTHER NODES ARE WORKERS
mkdir -p $SLURM_SUBMIT_DIR/$SLURM_JOB_ID
for i in `seq 0 $LAST`; do
  ssh ${NODES[$i]} "cd $SPARK_HOME; nohup ./bin/spark-class org.apache.spark.deploy.worker.Worker $MASTER &> $SLURM_SUBMIT_DIR/$SLURM_JOB_ID/nohup-${NODES[$i]}.$i.out" &
done

# SUBMIT PYSPARK JOB
$SPARK_HOME/bin/spark-submit --executor-cores $executor_cores --driver-memory "2G" --executor-memory "4G" --conf spark.storage.memoryFraction=0.2 --class "OptimalKnn" --master $MASTER $PROG $ARGS 
####$SPARK_HOME/bin/spark-submit --executor-cores $executor_cores --driver-memory "2G" --executor-memory "2G" --class "OptimalKnn" --master $MASTER $PROG $ARGS 



# CLEAN SPARK JOB
ssh ${NODES[0]} "cd $SPARK_HOME; ./sbin/stop-master.sh"

for i in `seq 0 $LAST`; do
  ssh ${NODES[$i]} "killall java"
done
