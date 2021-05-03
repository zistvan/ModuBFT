mkdir lat-logs

echo "VAL PEERS CLIENTS TPUT NBT"
for value in 512 1024 2048 4096 8192
do
	for peers in 3 5 7 9 11 13 15 
	do
		for clients in 1 2 4 6 8 10 12 14 16 
		do
			./run-apollo.sh $value $peers $clients > /tmp/apollo.log
			tput=`cat /tmp/apollo.log | grep "TPUT" | awk '{print $2}'`
			nbt=`cat /tmp/apollo.log | grep "NBT" | awk '{print $2}'`
			echo "$value $peers $clients $tput $nbt"
		done
	done

done