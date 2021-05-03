async_factor=32
use_pk_to_client="false"

machines="10.10.5.31,10.10.5.32,10.10.5.33,10.10.5.34,10.10.5.35,10.10.5.36,10.10.5.37,10.10.5.38,10.10.5.39,10.10.5.40,10.10.5.41,10.10.5.42,10.10.5.61,10.10.5.62,10.10.5.63,10.10.5.64,10.10.5.65,10.10.5.66,10.10.5.67,10.10.5.68,10.10.5.69,10.10.5.70,10.10.5.71,10.10.5.72,10.10.5.201,10.10.5.202,10.10.5.203,10.10.5.204,10.10.5.205,10.10.5.206,10.10.5.207,10.10.5.208"
num_machines=32

if [ ! -f ./cli-time-lat ]; then
    echo "Client file not found"
	exit
fi
if [ ! -f ./cli-time ]; then
    echo "Client file not found"
	exit
fi

echo -n "Cleaning up machines  "
for x in `seq 1 $num_machines`
do
	host=`echo $machines | cut -d ',' -f$x`
    ssh $host "killall cli-time" 2> /dev/null
    ssh $host "killall cli-time-lat" 2> /dev/null
	ssh $host "rm  /tmp/cli-*" 2> /dev/null
	echo -n "."
done
echo " done!"
sleep 2

logname=./`date -Is`.log

echo "DBG: -------------------------------------------" > $logname
echo "DBG: `date`" >> $logname
echo "DBG: Machines: $machines" >> $logname
echo "DBG: Async messages: $async_factor" >> $logname
echo "DBG: Using PK for client answer: $use_pk_to_client" >> $logname
echo "DBG: -------------------------------------------" >> $logname
echo "" >> $logname

cat $logname

echo "VAL PEERS CLIENTS TPUT NBT" >> $logname
echo "VAL PEERS CLIENTS TPUT NBT"
for value in 512 1024 2048 4096 8192
do
	for peers in 3 5 7 9 11 13 15 
	do
		for clients in 1 2 4 6 8 10 12 14 16 
		do
			./run-one-exp.sh $num_machines $machines $value $peers $clients $async_factor $use_pk_to_client > /tmp/$logname
			tput=`cat /tmp/$logname | grep "TPUT" | awk '{print $2}'`
			nbt=`cat /tmp/$logname | grep "NBT" | awk '{print $2}'`
			echo "$value $peers $clients $tput $nbt" >> $logname
			echo "$value $peers $clients $tput $nbt" 
		done
	done

done