n=8
machines="10.10.5.201,10.10.5.202,10.10.5.203,10.10.5.204,10.10.5.205,10.10.5.206,10.10.5.207,10.10.5.208"


num_peer=$2
num_client=$3
total_nodes=$((num_peer+num_client))

size=$1

role_list="2"

for r in `seq 2 $num_peer`
do
	role_list="$role_list,1"
done

for r in `seq 1 $num_client`
do
	role_list="$role_list,0"
done

ip_list=`echo $machines | cut -d ',' -f1`

for id in `seq 2 $total_nodes` 
do
	nth=`echo $machines | cut -d ',' -f$id`
	ip_list="$ip_list,$nth"
done

echo "NODES: $ip_list"
echo "ROLES: $role_list"

echo -n "COPY: "
for x in `seq 1 $total_nodes`
do
	host=`echo $machines | cut -d ',' -f$x`	
    scp ./cli* $host:/tmp/ > /dev/null
    echo -n "$x "
done
echo ""


specclient=$((num_peer+1))
nextclient=$((num_peer+2))

for x in `seq 1 $num_peer`
do
	host=`echo $machines | cut -d ',' -f$x`
    nodeId=$((x-1))    
    (ssh $host "/tmp/cli-time $nodeId $ip_list $role_list $size" | grep "Through" | awk '{print $3}' > /tmp/$host.log &)
done

for x in `seq $nextclient $total_nodes`
do
	host=`echo $machines | cut -d ',' -f$x`
    nodeId=$((x-1))    
    (ssh $host "/tmp/cli-time $nodeId $ip_list $role_list $size" | grep "Through" | awk '{print $3}' > /tmp/$host.log &)
done

host=`echo $machines | cut -d ',' -f$specclient`
ssh $host "/tmp/cli-time-lat $num_peer $ip_list $role_list $size 1" #| grep "Through" | awk '{print $3}' > /tmp/$host.log 

sleep 1

echo -n "CLEANUP: " 

for x in `seq 1 $total_nodes`
do
	host=`echo $machines | cut -d ',' -f$x`
    ssh $host "killall cli*" 2> /dev/null
    echo -n "$x "
done
echo ""

nbt=0
tput=0
for x in `seq $total_nodes -1 $specclient`
do
	host=`echo $machines | cut -d ',' -f$x`
	frac=`cat /tmp/$host.log`
	tput=$((tput+frac))
	nbt=`cat /tmp/$host.log`
	rm /tmp/$host.log
done

echo "TPUT: $tput"
echo "NBT: $nbt"


