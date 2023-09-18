source /etc/profile.d/hadoop.sh
source /etc/profile.d/spark.sh

spark-submit --num-executors 2 \
        --executor-cores 2 \
        --executor-memory 1024M \
        main.py