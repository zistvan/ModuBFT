package main

import (
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	n "github.com/zistvan/node"
	pb "github.com/zistvan/proto"
)

func main() {

	/* the Command Line Interface  can be used to start all nodes of a network, both peers and clients.

	It takes the following arguments:
	1 - nodeID, start nodes from zero
	2 - list of nodes defined by IP addresses, seprarated by comma. The list is the same for all. The location in the list == nodeID
	3 - role of each node, separeted by comma. ROLE constants defined in Node.
	4 - length of client requests in Bytes. This is only needed when starting a client
	5 - optional but must be 1 otherwise error! this is kept for compatibility with other clients
	6 - optional runtime (default 10s)
	*/

	myId, _ := strconv.Atoi(os.Args[1])
	nodeList := strings.Split(os.Args[2], ",")
	nodeRoleS := strings.Split(os.Args[3], ",")
	nodeRole := make([]int, len(nodeRoleS))
	for ind, elem := range nodeRoleS {
		nodeRole[ind], _ = strconv.Atoi(elem)
	}

	runTime := 10

	if len(os.Args) > 5 {
		batchF := 1
		batchF, _ = strconv.Atoi(os.Args[5])
		if batchF != 1 {
			fmt.Println("ERROR: THIS IS A LATENCY MEASURING CLIENT !")
			os.Exit(0)
		}
	}

	if len(os.Args) > 6 {
		runTime, _ = strconv.Atoi(os.Args[6])
	}

	me := &n.Node{}

	me.Initialize(myId, nodeList, nodeRole)
	me.Run()

	// if the node is to be a client...
	if nodeRole[myId] == n.ROLE_CLIENT {

		time.Sleep(time.Millisecond * 2000)

		//create a random value of specified length
		vallen, _ := strconv.Atoi(os.Args[4])
		rtxt := make([]byte, vallen)
		if _, err := io.ReadFull(rand.Reader, rtxt); err != nil {
			fmt.Println(err)
		}

		//define the length of the experiment

		//the benchmarking client can issue at most this many outstanding requests.

		chResult := make(chan int, 1000)

		//start a parallel goroutine for async message receival
		go me.ClientReceiveFromPeers(chResult)

		//send one message synchronously to generate crypto signatures -- these will be reused for benchmarking to avoid recomputation...
		_, _, rawB := me.ClientSendToLeader(rtxt, pb.SigType_PKSig)
		<-chResult

		issued := 0
		received := 0

		start := time.Now()

		elapsed := time.Since(start)

		latency := make([]int, 1001)

		for {
			st := time.Now()
			//me.ClientSendToLeaderCached(rtxt, pb.SigType_PKSig, sign)

			me.ClientSendToLeaderCachedRaw(rawB)
			issued++

			<-chResult
			received++

			tenUSecs := int(time.Since(st)/time.Microsecond) / 10
			if tenUSecs < 1000 {
				latency[tenUSecs]++
			} else {
				latency[1000]++
			}

			if issued%(100) == 0 {
				elapsed = time.Since(start)
				if int(elapsed) > (int(time.Second) * runTime) {
					break
				}
			}
		}

		elapsed = time.Since(start)
		tput := float32(received) / (float32(elapsed) / 1000000000.0)
		fmt.Printf("\nThroughput [ops/s] %d\n", int(tput))

		fmt.Println("USEC\tCOUNT")
		for x := 0; x < 1001; x++ {
			if latency[x] != 0 {
				fmt.Println(x*10, "\t", latency[x])
			}
		}
		os.Exit(0)

	}

}
