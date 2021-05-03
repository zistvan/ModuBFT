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
	*/

	myId, _ := strconv.Atoi(os.Args[1])
	nodeList := strings.Split(os.Args[2], ",")
	nodeRoleS := strings.Split(os.Args[3], ",")
	nodeRole := make([]int, len(nodeRoleS))
	for ind, elem := range nodeRoleS {
		nodeRole[ind], _ = strconv.Atoi(elem)
	}

	batchF := 64
	cnt := 10000

	if len(os.Args) > 5 {
		batchF, _ = strconv.Atoi(os.Args[5])
	}

	if len(os.Args) > 6 {
		cnt, _ = strconv.Atoi(os.Args[6])
	}

	fmt.Printf("Batch size: %d - total counter: %d\n", batchF, cnt)

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

		//start := time.Now()

		//the benchmarking client can issue at most this many outstanding requests.

		chResult := make(chan int, 1000)

		//start a parallel goroutine for async message receival
		go me.ClientReceiveFromPeers(chResult)

		//send one message synchronously to generate crypto signatures -- these will be reused for benchmarking to avoid recomputation...
		_, _, rawB := me.ClientSendToLeader(rtxt, pb.SigType_PKSig)
		<-chResult

		issued := 0
		received := 0
		committed := 0
		result := 0

		for received < cnt {

			for i := 0; i < batchF && issued < cnt; i++ {
				//st := time.Now()
				//me.ClientSendToLeaderCached(rtxt, pb.SigType_PKSig, sign)
				me.ClientSendToLeaderCachedRaw(rawB)
				issued++
			}
			go me.SendTimeOut(chResult, issued)

			//fmt.Println(time.Since(st))
			//st = time.Now()
			for received < issued-batchF/2 || (received < cnt && issued == cnt) {
				//fmt.Println(x + i)
				result = <-chResult
				if result > 0 {
					received++
					committed++
				} else if result == -1 {
					_, _, rawB = me.ClientSendToLeader(rtxt, pb.SigType_PKSig)
					//time.Sleep(2000 * time.Millisecond)
				} else if result == -issued {
					fmt.Printf("Timeout %d\n", issued)
					break
				}
			}
			received = issued
			//fmt.Prntln(time.Since(st))

			/* if received%(batchF*100) == 0 {
				fmt.Print(".")
			} */
		}

		//elapsed := time.Since(start)
		//tput := float32(committed) / (float32(elapsed) / 100000000.0)
		//fmt.Printf("\nThroughput [ops/s] %d\n", int(tput))
		os.Exit(0)

	}

}
