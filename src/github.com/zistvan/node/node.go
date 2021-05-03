package peer

// only needed below for sample processing

import (
	"bufio"
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"

	"encoding/binary"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	//"runtime/pprof"

	"github.com/golang/protobuf/proto"

	cr "github.com/zistvan/crypto"
	pb "github.com/zistvan/proto"
)

const (
	PORT_CLIENT = 12340
	PORT_PEER   = 12341
	PORT_COORD  = 12342
)

const (
	MAX_SIG_VERIF        = 10 //6
	MAX_SIG_CREATE       = 8  // 8 there's 3x this number of threads in verification
	MAX_SIG_VERIF_CLIENT = 20 //20
	MAXBUF               = 2048 * 2048
	CHECKPOINT_INTERVAL  = 500
	CIRCULAR_BUFFER_SIZE = 1000
	LOG_SIZE             = 10000
)

const (
	ROLE_CLIENT        = 0
	ROLE_PEER_FOLLOWER = 1
	ROLE_PEER_LEADER   = 2
	ROLE_COORDINATOR   = 3
)

const (
	//in milliseconds
	TIMEOUT_CLIENT = 100
)

type MessageWrapper struct {
	msg    *pb.PeerMessage
	sendTo []int
	hash   [32]byte
}

type PeerState struct {
	nodeId             int
	initialized        bool
	maxPrepared        int
	digestState        []byte
	digestCertificates []byte
	maxEpoch           int
}

type Node struct {
	MACKeys   [][]byte
	MACCipher []cipher.AEAD
	MACNonce  [][]byte

	PubKey     []string
	PKVerifier []cr.Unsigner
	myPrivKey  string
	PKSigner   cr.Signer

	peerCount    int
	nodeCount    int
	coordPresent bool
	nodeAddr     []string
	nodeRole     []int
	nodeConn     []net.Conn
	clientCount  int
	deadClients  int
	leaderId     int
	coordId      int

	//debugging
	reconfigured bool

	cntPrepare   []int
	cntCommit    []int
	senderOf     []int
	lastPrepared int

	myRole      int
	myEpoch     int
	nextPropSeq int
	myCommitSeq int
	myID        int
	nextCommit  int
	wedged      bool
	f           int

	chInData       chan *pb.PeerMessage
	chClientInData chan *pb.PeerMessage
	chCoordInData  chan *pb.PeerMessage
	chConEvent     chan int

	clientMsgDigests [][]byte
	clientMsgData    [][]byte
	commitLog        []pb.PeerMessage
	preparedLog      [CIRCULAR_BUFFER_SIZE][]pb.PeerMessage
	checkpointMsgs   [2][]pb.PeerMessage

	lastCheckpointSeq    int
	cntPendingCheckpoint int
	lastCheckpointHash   [32]byte
	runningStateHash     [32]byte
	usePKToClient        bool
}

// Initialize sets up a node and tells it about all other nodes in the network and their roles. Both consensus peers and clients are defined at startup time and put here. The node list must contain all clients at the end.
func (p *Node) Initialize(myID int, nodeAddr []string, nodeRoles []int) {

	if os.Getenv("BFT_PK_CLI") == "true" {
		p.usePKToClient = true
	} else {
		p.usePKToClient = false
	}
	p.myID = myID
	p.myEpoch = 0
	p.nextPropSeq = 0
	p.myRole = nodeRoles[myID]
	p.wedged = false
	p.reconfigured = false

	p.nodeRole = nodeRoles
	p.nodeAddr = nodeAddr

	p.clientCount = 0
	p.peerCount = 0

	p.coordPresent = false

	for i, role := range nodeRoles {
		if role == ROLE_CLIENT {
			p.clientCount++
		} else if role == ROLE_PEER_LEADER || role == ROLE_PEER_FOLLOWER {
			p.peerCount++
			if role == ROLE_PEER_LEADER {
				p.leaderId = i
			}
		} else if role == ROLE_COORDINATOR {
			p.coordPresent = true
			p.coordId = i
		}
	}

	p.f = (p.peerCount - 1) / 2

	p.nodeConn = make([]net.Conn, p.peerCount)

	p.nodeCount = len(nodeRoles)
	nodeCount := p.nodeCount

	p.myPrivKey = ""
	p.PubKey = make([]string, nodeCount)
	p.PKVerifier = make([]cr.Unsigner, nodeCount)

	for i := 0; i < nodeCount; i++ {
		p.PubKey[i] = ""
	}

	p.MACKeys = make([][]byte, nodeCount)
	p.MACCipher = make([]cipher.AEAD, nodeCount)
	p.MACNonce = make([][]byte, nodeCount)

	//we generate different MAC keys for each peer
	for i := 0; i < nodeCount; i++ {
		basePhrase := []byte("passphrasewhichneedstobe32bytes!")
		basePhrase[0] = byte(myID + int(i))
		p.MACKeys[i] = basePhrase
	}

	for i := 0; i < nodeCount; i++ {
		cypher, _ := aes.NewCipher(p.MACKeys[i])
		gcm, _ := cipher.NewGCM(cypher)
		p.MACCipher[i] = gcm

		nonce := make([]byte, gcm.NonceSize())
		for x := 0; x < gcm.NonceSize(); x++ {
			nonce[x] = byte(p.myID + int(i))
		}
		p.MACNonce[i] = nonce
	}

	p.chInData = make(chan *pb.PeerMessage, 1000)
	p.chClientInData = make(chan *pb.PeerMessage, 1000)
	p.chCoordInData = make(chan *pb.PeerMessage, 1000)
	p.chConEvent = make(chan int)

	if p.myRole == ROLE_CLIENT {
		fmt.Printf("Initialized as CLIENT. Id: %d\n", p.myID)
	} else if p.myRole == ROLE_PEER_FOLLOWER {
		fmt.Printf("Initialized as FOLLOWER. Id: %d\n", p.myID)
	} else if p.myRole == ROLE_PEER_LEADER {
		fmt.Printf("Initialized as LEADER. Id: %d\n", p.myID)
	} else if p.myRole == ROLE_COORDINATOR {
		fmt.Printf("Initialized as COORDINATOR. Id: %d\n", p.myID)
	}

	p.clientMsgData = make([][]byte, LOG_SIZE)
	p.clientMsgDigests = make([][]byte, LOG_SIZE)

	p.commitLog = make([]pb.PeerMessage, LOG_SIZE)
	p.checkpointMsgs[1] = make([]pb.PeerMessage, 2*p.f+1)
	for x := 0; x < CIRCULAR_BUFFER_SIZE; x++ {
		p.preparedLog[x] = make([]pb.PeerMessage, 2*p.f+1)
	}
	p.lastCheckpointSeq = 0
	p.cntPendingCheckpoint = 0
}

// BringUp starts the node. If it is a client, it will connec to consensus peers and then return. If it is a peer, it will connect to others and wait for all clients to connect before returning.
func (p *Node) Run() {

	p.nodeConn = make([]net.Conn, p.nodeCount)

	if p.myRole == ROLE_CLIENT {
		time.Sleep(time.Millisecond * 3000)
		p.initConnectionsAsClient()
		return

	} else if p.myRole == ROLE_PEER_FOLLOWER || p.myRole == ROLE_PEER_LEADER {
		go p.openAndListen(PORT_CLIENT)
		go p.openAndListen(PORT_PEER)
		go p.openAndListen(PORT_COORD)

		time.Sleep(time.Millisecond * 2000)

		go p.initConnectionsAsPeer()

		waitingFor := p.peerCount + p.clientCount
		if p.coordPresent {
			waitingFor++
		}
		for waitingFor > 0 {
			<-p.chConEvent
			//fmt.Printf("Connection on port %d\n", conPort)
			waitingFor--
		}

		p.RunPeerLoop()

	} else if p.myRole == ROLE_COORDINATOR {

		go p.openAndListen(PORT_COORD)

		time.Sleep(time.Millisecond * 2000)

		go p.initConnectionsAsPeer()

		waitingFor := p.peerCount + 1
		for waitingFor > 0 {
			<-p.chConEvent
			//fmt.Printf("Connection on port %d\n", conPort)
			waitingFor--
		}

		p.RunCoordLoop()

	}

}

func (p *Node) initConnectionsAsClient() {

	for ind := 0; ind < p.peerCount; ind++ {
		//fmt.Println("Connecting to " + p.nodeAddr[ind] + ":" + strconv.Itoa(PORT_CLIENT))
		conn, err := net.Dial("tcp", p.nodeAddr[ind]+":"+strconv.Itoa(PORT_CLIENT))
		if err != nil {
			fmt.Printf("%s\n", err)
		} else {
			p.nodeConn[ind] = conn
			fmt.Println("Connected to " + p.nodeAddr[ind] + ":" + strconv.Itoa(PORT_CLIENT))
		}

	}
}

// openAndListen is used by peers and it will spin a new goroutine to receive messages on an incoming connections in an infiitie loop. All these will be pushed into a single channel
func (p *Node) openAndListen(port int) {

	fmt.Println("Listening on " + p.nodeAddr[p.myID] + ":" + strconv.Itoa(port))
	ln, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		fmt.Println(err)
		os.Exit(0)
	}
	for {
		inConn, _ := ln.Accept()

		raddr := inConn.RemoteAddr().String()

		chanToUse := p.chInData

		if port == PORT_CLIENT {
			p.deadClients = 0
			chanToUse = p.chClientInData

			for i := 0; i < p.nodeCount; i++ {
				if strings.HasPrefix(raddr, p.nodeAddr[i]) && p.nodeRole[i] == ROLE_CLIENT {
					p.nodeConn[i] = inConn
					fmt.Println("Connection from ", i, " "+raddr)
				}
			}
		} else if port == PORT_COORD {
			chanToUse = p.chCoordInData
		}

		go p.receiveOnConn(inConn, chanToUse)

		p.chConEvent <- port

	}
}

// receiveOnConn is an infinite loop in which a valid protobuf is attempted to be read from the TCP connection. It is unmarshalled and passed to the channel. No further validation of the message contents takes place here.
func (p *Node) receiveOnConn(conn net.Conn, ch chan *pb.PeerMessage) {

	leftover := make([]byte, 0)
	rcvData := make([]byte, MAXBUF)
	for {
		n, _ := conn.Read(rcvData)
		//s := time.Now()
		if n == 0 {
			fmt.Println("Client disconnect ", conn.RemoteAddr())
			p.deadClients++
			if p.deadClients == p.clientCount {
				//pprof.StopCPUProfile()
				os.Exit(0)
			}
			break

		}

		ll := len(leftover)
		if ll > 0 {
			n = n + ll
			smallRecv := rcvData[0 : MAXBUF-ll]
			rcvData = append(leftover, smallRecv...)
			leftover = make([]byte, 0)
			//fmt.Println("Prepended ", ll)
		}
		si := 0
		for si < n {
			lf := rcvData[si : si+4]
			pbSize := binary.LittleEndian.Uint32(lf)
			//fmt.Println(pbSize, si, n)
			if si+4+int(pbSize) <= n {
				msg := &pb.PeerMessage{}
				err := proto.Unmarshal(rcvData[si+4:si+4+int(pbSize)], msg)
				if err != nil {
					fmt.Printf("%s\n", err)
				} else {
					//if p.VerifySig(msg) == true {
					ch <- msg
					//fmt.Println("TIMEReceive ", time.Since(s))
					//s = time.Now()
					//}
				}
				si = si + 4 + int(pbSize)
			} else {
				//fmt.Println("Leftover ", n-si)
				leftover = make([]byte, n-si)
				copy(leftover, rcvData[si:n])
				break
			}
		}
	}

}

func (p *Node) initConnectionsAsPeer() {

	for ind := 0; ind < p.nodeCount; ind++ {
		portToConn := -1
		if p.nodeRole[ind] == ROLE_PEER_FOLLOWER || p.nodeRole[ind] == ROLE_PEER_LEADER {
			if p.myRole != ROLE_COORDINATOR {
				portToConn = PORT_PEER
			} else {
				portToConn = PORT_COORD
			}

		} else if p.nodeRole[ind] == ROLE_COORDINATOR {
			portToConn = PORT_COORD
		}
		if portToConn != -1 {
			conn, err := net.Dial("tcp", p.nodeAddr[ind]+":"+strconv.Itoa(portToConn))
			if err == nil {
				p.nodeConn[ind] = conn
				fmt.Println("Connected to peer ", ind, " "+p.nodeAddr[ind]+":"+strconv.Itoa(portToConn))
			} else {
				fmt.Print("Error at node " + strconv.Itoa(p.myID) + ": ")
				fmt.Println(err)
				os.Exit(0)
			}
		}
	}

}

// ClientSendToLeader is used to send a message from a client node to the leader node. It computes signatures, etc. inside. It does not wait for an answer and returns once the data is sent.
func (p *Node) ClientSendToLeader(rawData []byte, sigType pb.SigType) ([32]byte, []*pb.Authenticator, []byte) {

	msg := &pb.PeerMessage{FromNodeId: int32(p.myID), Type: pb.MsgType_ClientRequest, AttachedData: rawData}
	sendTo := p.leaderId
	hash := p.computeMsgHash(msg)
	sign := p.CreateSigForHash(hash, sigType, sendTo)
	msg.Auth = make([]*pb.Authenticator, 1)
	msg.Auth[0] = sign

	rawBytes, err := proto.Marshal(msg)
	if err != nil {
		fmt.Printf("%s\n", err)
	}
	lba := make([]byte, 4)
	binary.LittleEndian.PutUint32(lba, uint32(len(rawBytes)))
	p.nodeConn[p.leaderId].Write(append(lba, rawBytes...))
	return hash, msg.Auth, rawBytes
}

// ClientSendToLeaderCached does the same as ClientSendToLeader, but instead of computing the signatures for the message, it reuses an already computed one. Of course, the message contents have to remain identical.
func (p *Node) ClientSendToLeaderCached(rawData []byte, sigType pb.SigType, cachedSign []*pb.Authenticator) bool {

	msg := &pb.PeerMessage{FromNodeId: int32(p.myID), Type: pb.MsgType_ClientRequest, AttachedData: rawData}
	msg.Auth = cachedSign

	rawBytes, err := proto.Marshal(msg)
	if err != nil {
		fmt.Printf("%s\n", err)
	}
	lba := make([]byte, 4)
	binary.LittleEndian.PutUint32(lba, uint32(len(rawBytes)))
	n, err := p.nodeConn[p.leaderId].Write(append(lba, rawBytes...))
	if err != nil {
		fmt.Printf("%s %d\n", err, n)
		return false
	}

	return true
}

// ClientSendToLeaderCached does the same as ClientSendToLeader, but sends an already serialized message.
func (p *Node) ClientSendToLeaderCachedRaw(rawBytes []byte) bool {

	lba := make([]byte, 4)
	binary.LittleEndian.PutUint32(lba, uint32(len(rawBytes)))
	n, err := p.nodeConn[p.leaderId].Write(append(lba, rawBytes...))
	if err != nil {
		fmt.Printf("%s %d\n", err, n)
		return false
	}

	return true
}

func (p *Node) SendTimeOut(msgId chan int, slot int) {
	time.Sleep(TIMEOUT_CLIENT * time.Millisecond)
	msgId <- -slot
}

// ClientReceiveFromPeers starts a thread for each peer of the consensus and collects messages into a channel that is fed into a verification step. Finally, the method returns messageIDs as they are committed through a channel.
func (p *Node) ClientReceiveFromPeers(msgId chan int) {

	answers := make([]int, CIRCULAR_BUFFER_SIZE)
	epoch := make([]int, CIRCULAR_BUFFER_SIZE)
	last := -1

	verifMsgChan := make(chan *pb.PeerMessage, 1000)
	validMsgChan := make(chan *pb.PeerMessage, 1000)

	for x := 0; x < p.peerCount; x++ {
		go p.receiveOnConn(p.nodeConn[x], verifMsgChan)
	}

	go p.peerParallelVerifierStep(verifMsgChan, validMsgChan)

	for {
		msg := <-validMsgChan

		if msg == nil {
			fmt.Println("Received msg with invalid sig!")
			continue
		}

		if msg.MsgId == -1 {
			if p.leaderId != int(msg.FromNodeId) {
				fmt.Printf("Changing leader from %d to %d\n", p.leaderId, int(msg.FromNodeId))
				p.leaderId = int(msg.FromNodeId)
				msgId <- int(msg.MsgId)
				p.reconfigured = true
				//answers = make([]int, CIRCULAR_BUFFER_SIZE)
			}
		} else {
			if epoch[msg.MsgId%CIRCULAR_BUFFER_SIZE] < int(msg.EpochId) {
				answers[msg.MsgId%CIRCULAR_BUFFER_SIZE] = 1
				epoch[msg.MsgId%CIRCULAR_BUFFER_SIZE] = int(msg.EpochId)
			} else {
				answers[msg.MsgId%CIRCULAR_BUFFER_SIZE]++
			}

			//if p.reconfigured && int(msg.MsgId)%5000 == 1 {
			//	fmt.Printf("Received reply after reconfiguration %d from %d with last %d, total %d\n", msg.MsgId, int(msg.FromNodeId), last, answers[msg.MsgId%CIRCULAR_BUFFER_SIZE])
			//}

			//fmt.Println("Answer state of ", msg.MsgId, " is ", answers[msg.MsgId%1000])

			// need to collect ALL answers before we consider the message committed.
			if answers[msg.MsgId%CIRCULAR_BUFFER_SIZE] == p.peerCount {
				//if p.reconfigured && int(msg.MsgId)%5000 == 1 {
				//	fmt.Printf("Received full reply after reconfiguration %d from %d with last %d\n", msg.MsgId, int(msg.FromNodeId), last)
				//}
				if last < int(msg.MsgId) {
					last = int(msg.MsgId)
					msgId <- int(msg.MsgId)
				}
				answers[msg.MsgId%CIRCULAR_BUFFER_SIZE] = 0

			}
		}

	}
}

/*RunPeerLoop executes the peer pipeline in an infinite loop. It has the following stages:
- parallel receive, one thread per peer, feeding into a single channel from all other peers (chInData) and a single channel for all clients (chClientData)
- parallel verifier step takes maeesages from a channel and does a FIFO round-robin parallelization -- there's two instances, one for peers and one for clients. Both put verified messages into 'verifOut'.
- decision step -- single thread, defines what happens to each message
- parallel sender step -- sends a given message in parallel to different recipients. This means also computing signatures in parallel.

*/
func (p *Node) RunPeerLoop() {

	/*
		f, err := os.Create("/tmp/abft/cpuprof.prof")
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
	*/

	//runtime.GOMAXPROCS(20)

	peerVerifOut := make(chan *pb.PeerMessage, 1000)
	clientVerifOut := make(chan *pb.PeerMessage, 1000)
	coordVerifOut := make(chan *pb.PeerMessage, 1000)
	decOut := make(chan *MessageWrapper, 1000)

	p.cntPrepare = make([]int, CIRCULAR_BUFFER_SIZE)
	p.cntCommit = make([]int, CIRCULAR_BUFFER_SIZE)
	p.senderOf = make([]int, CIRCULAR_BUFFER_SIZE)

	go p.peerParallelVerifierStep(p.chInData, peerVerifOut)
	go p.peerParallelVerifierStep(p.chClientInData, clientVerifOut)
	go p.peerParallelVerifierStep(p.chCoordInData, coordVerifOut)

	go p.peerDecisionStep(peerVerifOut, clientVerifOut, coordVerifOut, decOut)

	p.peerParallelSenderStep(decOut)

}

func (p *Node) RunCoordLoop() {

	verifOut := make(chan *pb.PeerMessage, 1000)
	decOut := make(chan *MessageWrapper, 1000)

	p.cntPrepare = make([]int, CIRCULAR_BUFFER_SIZE)
	p.cntCommit = make([]int, CIRCULAR_BUFFER_SIZE)
	p.senderOf = make([]int, CIRCULAR_BUFFER_SIZE)

	go p.peerParallelVerifierStep(p.chCoordInData, verifOut)

	go p.coordDecisionStep(verifOut, decOut)

	p.peerParallelSenderStep(decOut)

}

func (p Node) peerVerifierStep(chIn chan *pb.PeerMessage, chOut chan *pb.PeerMessage) {
	for {
		msg := <-chIn
		//fmt.Printf(" 1) Received %d from %d\n", msg.Type, msg.FromNodeId)
		if p.VerifySig(msg) == true {
			chOut <- msg
		}
	}
}

// peerParallelVerifierStep distributes messages from the input channel in a round-robin fashion to several parallel verifiers. Results are collected the same way into a single channel.
func (p *Node) peerParallelVerifierStep(chIn chan *pb.PeerMessage, chOut chan *pb.PeerMessage) {

	verifChanIn := make([]chan *pb.PeerMessage, MAX_SIG_VERIF_CLIENT)
	verifChanOut := make([]chan *pb.PeerMessage, MAX_SIG_VERIF_CLIENT)
	for x := 0; x < MAX_SIG_VERIF_CLIENT; x++ {
		verifChanIn[x] = make(chan *pb.PeerMessage, 1000)
		verifChanOut[x] = make(chan *pb.PeerMessage, 1000)

		go func(chIn chan *pb.PeerMessage, chOut chan *pb.PeerMessage) {

			for {
				msg := <-chIn
				//s := time.Now()

				if p.VerifySig(msg) == true {
					chOut <- msg
				} else {
					chOut <- nil
				}

				//fmt.Println("TIMEVerif ", time.Since(s))
			}
		}(verifChanIn[x], verifChanOut[x])
	}

	go func() {
		cnt := 0
		for {
			msg := <-chIn
			verifChanIn[cnt%MAX_SIG_VERIF_CLIENT] <- msg
			cnt++
		}
	}()

	rc := 0
	for {
		tos := <-verifChanOut[rc%MAX_SIG_VERIF_CLIENT]
		if tos != nil {
			chOut <- tos
		}
		rc++
	}
}

func (p *Node) peerDecisionStep(chPeerIn chan *pb.PeerMessage, chClientIn chan *pb.PeerMessage, chCoordIn chan *pb.PeerMessage, chOut chan *MessageWrapper) {
	/**
	commitedMessages := 0
	startTime := time.Now()
	**/

	//server := sha256.NewAvx512Server()
	//h512 := sha256.NewAvx512(server)

	msg := &pb.PeerMessage{}

	for {

		if p.wedged == true {
			select {
			case msg = <-chPeerIn:
			case msg = <-chCoordIn:
			}
		} else {
			select {
			case msg = <-chPeerIn:
			case msg = <-chClientIn:
			case msg = <-chCoordIn:
			}
		}
		/*
			msgCnt++
			s := time.Now()
		*/
		if msg.Type == pb.MsgType_ClientRequest {
			if p.myRole == ROLE_PEER_LEADER {
				if p.nextPropSeq-p.lastCheckpointSeq > 2*CHECKPOINT_INTERVAL {
					//ERROR!!!
					fmt.Printf("The last checkpoint was at %d, but we try to propose %d\n", p.lastCheckpointSeq, p.nextPropSeq)
				} else {

					msgPP := &pb.PeerMessage{}
					msgPP.Type = pb.MsgType_PrePrepare
					msgPP.FromNodeId = int32(p.myID)
					msgPP.MsgId = int32(p.nextPropSeq)
					msgPP.EpochId = int32(p.myEpoch)

					rawMsg, err := proto.Marshal(msg)
					if err != nil {
						fmt.Printf("%s\n", err)
					}

					msgPP.AttachedData = rawMsg

					resp := &MessageWrapper{sendTo: make([]int, p.peerCount), msg: msgPP}
					c := 0
					for x := 0; x < p.peerCount; x++ {
						resp.sendTo[c] = x
						c++
					}

					p.nextPropSeq++

					chOut <- resp

				}
			} else {
				fmt.Printf("I am no the ledaer. MyId is %d, and the leader is %d\n", p.myID, p.leaderId)

				msgR := &pb.PeerMessage{}
				msgR.Type = pb.MsgType_ClientResponse
				msgR.FromNodeId = int32(p.leaderId)
				msgR.MsgId = int32(-1)
				msgR.EpochId = msg.EpochId

				resp := &MessageWrapper{sendTo: make([]int, 1), msg: msgR}
				resp.sendTo[0] = int(msg.FromNodeId)

				chOut <- resp
			}

		} else if msg.Type == pb.MsgType_PrePrepare && p.myEpoch == int(msg.EpochId) && p.wedged == false {

			msgP := &pb.PeerMessage{}
			msgP.Type = pb.MsgType_Prepare
			msgP.FromNodeId = int32(p.myID)
			msgP.MsgId = msg.MsgId
			msgP.EpochId = msg.EpochId

			shaOfClientMsg := sha256.Sum256(msg.AttachedData)
			p.clientMsgDigests[msg.MsgId%LOG_SIZE] = make([]byte, len(shaOfClientMsg))
			copy(p.clientMsgDigests[msg.MsgId%LOG_SIZE], shaOfClientMsg[:])

			//h512.Write(rawMsg)
			//sha := h512.Sum([]byte{})

			//saving the message to the log
			clMsg := &pb.PeerMessage{}
			proto.Unmarshal(msg.AttachedData, clMsg)
			p.senderOf[msg.MsgId%CIRCULAR_BUFFER_SIZE] = int(clMsg.FromNodeId)
			p.clientMsgData[msg.MsgId%LOG_SIZE] = make([]byte, len(msg.AttachedData))
			copy(p.clientMsgData[msg.MsgId%LOG_SIZE], msg.AttachedData)

			msgP.AttachedData = shaOfClientMsg[:]
			resp := &MessageWrapper{sendTo: make([]int, p.peerCount), msg: msgP}
			for x := 0; x < p.peerCount; x++ {
				resp.sendTo[x] = x
			}

			chOut <- resp

		} else if msg.Type == pb.MsgType_Prepare && p.myEpoch == int(msg.EpochId) && p.wedged == false {

			p.cntPrepare[msg.MsgId%CIRCULAR_BUFFER_SIZE]++
			p.preparedLog[msg.MsgId%(2*CHECKPOINT_INTERVAL)][msg.FromNodeId] = *p.clonePeerMessage(msg)

			if p.cntPrepare[msg.MsgId%CIRCULAR_BUFFER_SIZE] == p.peerCount {

				if bytes.Compare(msg.AttachedData, p.clientMsgDigests[msg.MsgId%LOG_SIZE][:]) != 0 {
					fmt.Println("ERROR in message digest! Expected \n", msg.AttachedData, "\nand got \n", p.clientMsgDigests[msg.MsgId%LOG_SIZE][:])
				} else {

					msgP := &pb.PeerMessage{}
					msgP.Type = pb.MsgType_Commit
					msgP.FromNodeId = int32(p.myID)
					msgP.MsgId = msg.MsgId
					msgP.EpochId = msg.EpochId

					msgP.AttachedData = msg.AttachedData

					resp := &MessageWrapper{sendTo: make([]int, p.peerCount-1), msg: msgP}
					c := 0
					for x := 0; x < p.peerCount; x++ {
						if x != p.myID {
							resp.sendTo[c] = x
							c++
						}
					}

					p.cntPrepare[msg.MsgId%CIRCULAR_BUFFER_SIZE] = 0
					p.lastPrepared = int(msg.MsgId)

					chOut <- resp
				}
			}

		} else if msg.Type == pb.MsgType_Commit && p.myEpoch == int(msg.EpochId) && p.wedged == false {

			if bytes.Compare(msg.AttachedData, p.clientMsgDigests[msg.MsgId%LOG_SIZE][:]) != 0 {
				fmt.Println("ERROR in message digest!")
			} else {

				p.cntCommit[msg.MsgId%CIRCULAR_BUFFER_SIZE]++
				//fmt.Println("Commit state of ", msg.MsgId, " is ", p.cntCommit[msg.MsgId%1000])
				//if p.reconfigured && (msg.MsgId%100 == 99 || msg.MsgId%100 == 0) {
				//	fmt.Printf("Received commit for %d after reconfigured. MyId: %d, from %d, total %d\n", int32(msg.MsgId), int32(p.myID), msg.FromNodeId, p.cntCommit[msg.MsgId%1000])
				//}
				if p.cntCommit[msg.MsgId%CIRCULAR_BUFFER_SIZE] == p.peerCount-1 && p.nextCommit == int(msg.MsgId) {

					for {
						msgR := &pb.PeerMessage{}
						msgR.Type = pb.MsgType_ClientResponse
						msgR.FromNodeId = int32(p.myID)
						msgR.MsgId = int32(p.nextCommit)
						msgR.EpochId = msg.EpochId

						resp := &MessageWrapper{sendTo: make([]int, 1), msg: msgR}
						resp.sendTo[0] = p.senderOf[p.nextCommit%CIRCULAR_BUFFER_SIZE]
						//if p.reconfigured && msg.MsgId%5000 == 1 {
						//	fmt.Printf("Committed %d after reconfigured. MyId: %d, from %d\n", int32(msg.MsgId), int32(p.myID), msg.FromNodeId)
						//}

						p.cntCommit[msg.MsgId%CIRCULAR_BUFFER_SIZE] = 0
						p.commitLog[msg.MsgId%CIRCULAR_BUFFER_SIZE] = *p.clonePeerMessageNoAuth(msg)

						/** STATS
						commitedMessages++
						if commitedMessages%2000 == 0 {
							fmt.Println("Throughput at node ", p.myID, ": ", (1000000000 * float32(commitedMessages) / float32(time.Since(startTime))), " (", (time.Since(startTime)), ")")
							commitedMessages = 0
							startTime = time.Now()
						}
						**/
						chOut <- resp

						p.runningStateHash = p.nextStateHash(p.runningStateHash, p.clientMsgDigests[msg.MsgId%LOG_SIZE])

						if p.nextCommit-p.lastCheckpointSeq == CHECKPOINT_INTERVAL {

							p.lastCheckpointHash = p.runningStateHash
							msgP := &pb.PeerMessage{}
							msgP.Type = pb.MsgType_Checkpoint
							msgP.FromNodeId = int32(p.myID)
							msgP.MsgId = int32(p.nextCommit)
							msgP.EpochId = int32(p.myEpoch)

							msgP.AttachedData = p.lastCheckpointHash[:]

							//fmt.Printf("Attempting checkpoint on peer %d at SEQ%d\n", p.myID, p.nextCommit)

							resp := &MessageWrapper{sendTo: make([]int, p.peerCount), msg: msgP}
							for x := 0; x < p.peerCount; x++ {
								resp.sendTo[x] = x
							}

							chOut <- resp
						}

						p.nextCommit++

						if p.cntCommit[(p.nextCommit)%CIRCULAR_BUFFER_SIZE] == p.peerCount-1 {
							continue
						} else {
							break
						}
					}

				}
			}

		} else if msg.Type == pb.MsgType_Checkpoint && p.myEpoch == int(msg.EpochId) && p.wedged == false {
			//fmt.Printf("Got checkpoint at peer %d from peer %d at SEQ%d\n", p.myID, int(msg.FromNodeId), int(msg.MsgId))
			if int(msg.MsgId) == p.lastCheckpointSeq+CHECKPOINT_INTERVAL {
				p.cntPendingCheckpoint++
				p.checkpointMsgs[1][msg.FromNodeId] = *p.clonePeerMessage(msg)
				if p.cntPendingCheckpoint == p.peerCount {
					p.lastCheckpointSeq = int(msg.MsgId)
					//fmt.Printf("Peer %d checkpointed at %d\n", p.myID, p.lastCheckpointSeq)
					p.cntPendingCheckpoint = 0
					p.checkpointMsgs[0] = p.checkpointMsgs[1]
				}
			}
		} else if msg.Type == pb.MsgType_Probe {
			fmt.Println("Coordinator says: ", string(msg.AttachedData))

			if p.nodeRole[msg.FromNodeId] != ROLE_COORDINATOR {
				fmt.Println("ERROR!!!!")
			}

			msgR := &pb.PeerMessage{}
			msgR.Type = pb.MsgType_ProbeAck
			msgR.FromNodeId = int32(p.myID)
			msgR.MsgId = msg.MsgId
			msgR.EpochId = msg.EpochId

			cntPreparedCertificates := p.lastPrepared - p.lastCheckpointSeq

			msgC := &pb.CertificateMessage{}
			msgC.Initialized = true
			msgC.CntPrepared = int32(cntPreparedCertificates)
			msgC.Certificates = make([]*pb.PeerMessage, cntPreparedCertificates*p.peerCount+p.peerCount)

			for x := 0; x < p.peerCount; x++ {
				msgC.Certificates[x] = &p.checkpointMsgs[0][x]
			}
			//fmt.Printf("checkpoint sequence %d, starting prepared %d, end prepared %d\n", p.lastCheckpointSeq, p.lastCheckpointSeq+1, p.lastCheckpointSeq+cntPreparedCertificates+1)
			index := 1
			for x := p.lastCheckpointSeq + 1; x < p.lastCheckpointSeq+cntPreparedCertificates+1; x++ {
				for y := 0; y < p.peerCount; y++ {
					msgC.Certificates[index*p.peerCount+y] = &p.preparedLog[x%(2*CHECKPOINT_INTERVAL)][y]
					//fmt.Printf("prepared msg: %d, peer %d, index %d, size %d\n", x, y, index, cntPreparedCertificates*p.peerCount+p.peerCount)
				}
				index++
			}
			rawMsg, err := proto.Marshal(msgC)
			if err != nil {
				fmt.Printf("Marshalling certificates error: %s\n", err)
			}

			msgR.AttachedData = rawMsg

			resp := &MessageWrapper{sendTo: make([]int, 1), msg: msgR}
			resp.sendTo[0] = int(msg.FromNodeId)

			chOut <- resp

			p.wedged = true

		} else if msg.Type == pb.MsgType_NewConfig && p.wedged == true {
			msgP := &pb.PayloadNewConfig{}
			err := proto.Unmarshal(msg.AttachedData, msgP)
			if err != nil {
				fmt.Printf("%s\n", err)
			}

			if int(msgP.Leader) == p.myID {

				fmt.Printf("Received NewConfig. Peer: %d\n", p.myID)

				p.reconfigured = true

				//Resetting the internal state
				p.nextPropSeq = p.lastPrepared + 1
				p.nextCommit = p.lastCheckpointSeq + 1

				p.cntPrepare = make([]int, CIRCULAR_BUFFER_SIZE)
				p.cntCommit = make([]int, CIRCULAR_BUFFER_SIZE)

				p.cntPendingCheckpoint = 0

				//commitedMessages = 0

				//Setting new role and unwedging
				p.wedged = false
				p.myRole = ROLE_PEER_LEADER
				p.myEpoch = int(msg.EpochId)
				p.leaderId = p.myID

				//Sending SyncState to new followers
				msgR := &pb.PeerMessage{}
				msgR.Type = pb.MsgType_SyncState
				msgR.FromNodeId = int32(p.myID)
				msgR.MsgId = 0
				msgR.EpochId = int32(p.myEpoch)

				//Creating the message payload, including the NewConfig message and the payload of the probe_ack msg that the peer sent to the reconfiguration service
				payload := &pb.PayloadSyncState{}
				payload.NewConfigMessage = msg
				payload.Certificate = &pb.CertificateMessage{}

				payload.Certificate.Initialized = true

				cntPreparedCertificates := p.lastPrepared - p.lastCheckpointSeq

				payload.Certificate.CntPrepared = int32(cntPreparedCertificates)
				payload.Certificate.Certificates = make([]*pb.PeerMessage, cntPreparedCertificates*p.peerCount+p.peerCount)
				payload.Clients = make([]int32, cntPreparedCertificates)

				for x := 0; x < p.peerCount; x++ {
					payload.Certificate.Certificates[x] = &p.checkpointMsgs[0][x]
				}
				//fmt.Printf("checkpoint sequence %d, starting prepared %d, end prepared %d\n", p.lastCheckpointSeq, p.lastCheckpointSeq+1, p.lastCheckpointSeq+cntPreparedCertificates+1)
				index := 1
				for x := p.lastCheckpointSeq + 1; x < p.lastCheckpointSeq+cntPreparedCertificates+1; x++ {
					for y := 0; y < p.peerCount; y++ {
						payload.Certificate.Certificates[index*p.peerCount+y] = &p.preparedLog[x%(2*CHECKPOINT_INTERVAL)][y]
						//fmt.Printf("prepared msg: %d, peer %d, index %d, size %d\n", x, y, index, cntPreparedCertificates*p.peerCount+p.peerCount)
					}
					payload.Clients[index-1] = int32(p.senderOf[x%CIRCULAR_BUFFER_SIZE])
					index++
				}

				rawMsg, err := proto.Marshal(payload)
				if err != nil {
					fmt.Printf("Marshalling certificates error: %s\n", err)
				}
				msgR.AttachedData = rawMsg

				resp := &MessageWrapper{sendTo: make([]int, p.peerCount-1), msg: msgR}
				c := 0
				for x := 0; x < p.peerCount; x++ {
					if x != p.myID {
						resp.sendTo[c] = x
						c++
					}
				}

				chOut <- resp

				//send commit after SyncState message
				for x := p.lastCheckpointSeq + 1; x <= p.lastPrepared; x++ {

					msgP := &pb.PeerMessage{}
					msgP.Type = pb.MsgType_Commit
					msgP.FromNodeId = int32(p.myID)
					msgP.MsgId = int32(x)
					msgP.EpochId = int32(p.myEpoch)

					msgP.AttachedData = p.preparedLog[x%(2*CHECKPOINT_INTERVAL)][0].AttachedData

					resp := &MessageWrapper{sendTo: make([]int, p.peerCount-1), msg: msgP}
					c := 0
					for x := 0; x < p.peerCount; x++ {
						if x != p.myID {
							resp.sendTo[c] = x
							c++
						}
					}

					chOut <- resp
				}
			}

		} else if msg.Type == pb.MsgType_SyncState && p.wedged == true {
			fmt.Printf("Received SyncState. Peer: %d\n", p.myID)

			p.reconfigured = true

			payload := &pb.PayloadSyncState{}
			err1 := proto.Unmarshal(msg.AttachedData, payload)
			if err1 != nil {
				fmt.Printf("%s\n", err1)
			}

			payloadNewConfig := &pb.PayloadNewConfig{}
			err1 = proto.Unmarshal(payload.NewConfigMessage.AttachedData, payloadNewConfig)
			if err1 != nil {
				fmt.Printf("%s\n", err1)
			}

			msgBytes, err2 := proto.Marshal(payload.Certificate)
			if err2 != nil {
				fmt.Printf("%s\n", err2)
			}
			msgHash := sha256.Sum256(msgBytes)

			//if p.VerifySig(payload.NewConfigMessage) && bytes.Compare(msgHash[:], payloadNewConfig.DigestP) == 0 {
			if bytes.Compare(msgHash[:], payloadNewConfig.DigestP) == 0 {

				leaderCheckpointSeq := int(payload.Certificate.Certificates[0].MsgId)
				leaderPreparedSeq := int(payload.Certificate.CntPrepared) + leaderCheckpointSeq
				if p.lastCheckpointSeq != leaderCheckpointSeq {
					fmt.Printf("Different snapshot. Leader: %d, Mine: %d\n", leaderCheckpointSeq, p.lastCheckpointSeq)
				}
				if p.lastPrepared+1 == leaderPreparedSeq {
					p.lastPrepared++
					index := int(payload.Certificate.CntPrepared) * p.peerCount
					//fmt.Printf("client %d for %d\n", int(payload.Clients[len(payload.Clients)-1]), leaderPreparedSeq)
					p.senderOf[leaderPreparedSeq%CIRCULAR_BUFFER_SIZE] = int(payload.Clients[len(payload.Clients)-1])
					for x := 0; x < p.peerCount; x++ {
						p.preparedLog[leaderPreparedSeq%(2*CHECKPOINT_INTERVAL)][x] = *p.clonePeerMessage(payload.Certificate.Certificates[index+x])
					}

				} else if p.lastPrepared+1 < leaderPreparedSeq {
					fmt.Printf("WEIRD: Different number of prepared. Leader up to: %d, Mine: %d\n", leaderPreparedSeq, p.lastPrepared)
				} else {
					fmt.Printf("Same number of prepared. Leader up to: %d, Mine: %d\n", leaderPreparedSeq, p.lastPrepared)
				}
				//Resetting the internal state
				p.cntPrepare = make([]int, CIRCULAR_BUFFER_SIZE)
				p.cntCommit = make([]int, CIRCULAR_BUFFER_SIZE)
				p.cntPendingCheckpoint = 0
				p.nextCommit = leaderCheckpointSeq + 1
				//commitedMessages = 0

				//Setting new role and unwedging
				p.wedged = false
				p.myRole = ROLE_PEER_FOLLOWER
				p.myEpoch = int(msg.EpochId)
				p.leaderId = int(payloadNewConfig.Leader)

				//send commit after SyncState message
				for x := p.lastCheckpointSeq + 1; x <= p.lastPrepared; x++ {

					msgP := &pb.PeerMessage{}
					msgP.Type = pb.MsgType_Commit
					msgP.FromNodeId = int32(p.myID)
					msgP.MsgId = int32(x)
					msgP.EpochId = int32(p.myEpoch)

					msgP.AttachedData = p.preparedLog[x%(2*CHECKPOINT_INTERVAL)][0].AttachedData

					resp := &MessageWrapper{sendTo: make([]int, p.peerCount-1), msg: msgP}
					c := 0
					for x := 0; x < p.peerCount; x++ {
						if x != p.myID {
							resp.sendTo[c] = x
							c++
						}
					}

					chOut <- resp
				}

			}
		} else {
			fmt.Printf("Rejected %d from %d, msgId %d, epoch %d\n", msg.Type, msg.FromNodeId, msg.MsgId, msg.EpochId)
		}
		/*
			elap = elap + time.Since(s)

			if msgCnt%100000 == 0 {
				fmt.Println("DEC ", int(elap)/int(time.Microsecond)/msgCnt)
			}/*
			/*if elap > time.Microsecond {
				fmt.Printf("  2) Received %d from %d, msgId %d\n", msg.Type, msg.FromNodeId, msg.MsgId)
				fmt.Println("TIMEDecide ", elap)
			}*/
	}
}

func (p *Node) coordDecisionStep(chIn chan *pb.PeerMessage, chOut chan *MessageWrapper) {

	probing := false
	remF := p.f
	countAnswers := 0
	states := make([]PeerState, p.f+1)

	for {
		if !probing {
			reader := bufio.NewReader(os.Stdin)
			text, _ := reader.ReadString('\n')
			request, err := strconv.Atoi(text[:1])
			if request == 0 && err == nil {
				probing = true
				msgP := &pb.PeerMessage{}
				msgP.Type = pb.MsgType_Probe
				msgP.FromNodeId = int32(p.myID)
				msgP.MsgId = 0
				msgP.EpochId = int32(p.myEpoch + 1)

				msgP.AttachedData = []byte(text[:1])

				wrapP := &MessageWrapper{sendTo: make([]int, p.peerCount), msg: msgP}
				for x := 0; x < p.peerCount; x++ {
					wrapP.sendTo[x] = x
				}

				chOut <- wrapP
			} else {
				fmt.Println("Request not matching: " + text + ". Please enter 0 to reconfigure.")

			}

		} else {

			msg := <-chIn

			if msg.Type == pb.MsgType_ProbeAck {
				result, state := p.checkAndExtract(msg)
				if result {
					states[countAnswers] = state
					countAnswers++
					if countAnswers == remF+1 {

						if p.isOperational(states, remF) {
							countAnswers = 0
							probing = false
							remF = p.f

							_, leader, ds, dp := p.computeMembership(states)
							//p.myEpoch++

							msgPayload := &pb.PayloadNewConfig{}
							msgPayload.Leader = int32(leader)
							msgPayload.DigestState = ds
							msgPayload.DigestP = dp
							//copy(dp, msgPayload.DigestP)
							msgR := &pb.PeerMessage{}
							msgR.Type = pb.MsgType_NewConfig
							msgR.FromNodeId = int32(p.myID)
							msgR.MsgId = 0
							msgR.EpochId = int32(p.myEpoch + 1)

							rawMsg, err := proto.Marshal(msgPayload)
							if err != nil {
								fmt.Printf("Marshalling certificates error: %s\n", err)
							}

							msgR.AttachedData = rawMsg

							resp := &MessageWrapper{sendTo: make([]int, p.peerCount), msg: msgR}
							for x := 0; x < p.peerCount; x++ {
								resp.sendTo[x] = x
							}

							chOut <- resp

						} else {
							fmt.Println("Not operational or not sure at least")
						}
					}
				} else {
					remF = remF - 1
				}
			}
		}
	}
}

// peerParallelSenderStep computes signatures on several threads (round robin distribution from chIn to signQueues). The results og the signatures (sendQueues) are read round-robin and enqueued to their corresponding peer queu (peerQueue). Each peer queue has its associates sending thread.
func (p *Node) peerParallelSenderStep(chIn chan *MessageWrapper) {

	hashInQueues := make([]chan *MessageWrapper, MAX_SIG_CREATE)
	hashOutQueues := make([]chan *MessageWrapper, MAX_SIG_CREATE)
	signQueues := make([]chan *MessageWrapper, MAX_SIG_CREATE)
	sendQueues := make([]chan *MessageWrapper, MAX_SIG_CREATE)
	peerQueues := make([]chan *MessageWrapper, p.nodeCount)

	for x := 0; x < MAX_SIG_CREATE; x++ {
		hashInQueues[x] = make(chan *MessageWrapper, 1000)
		hashOutQueues[x] = make(chan *MessageWrapper, 1000)
		signQueues[x] = make(chan *MessageWrapper, 1000)
		sendQueues[x] = make(chan *MessageWrapper, 1000)
		sendQueues[x] = make(chan *MessageWrapper, 1000)
	}
	for x := 0; x < p.nodeCount; x++ {
		peerQueues[x] = make(chan *MessageWrapper, 1000)
	}
	//server := make([]*sha256.Avx512Server, MAX_SIG_CREATE)

	for x := 0; x < MAX_SIG_CREATE; x++ {

		go func(chIn <-chan *MessageWrapper, chOut chan<- *MessageWrapper) { //, srv *sha256.Avx512Server) {

			for {
				wrap := <-chIn
				//s := time.Now()

				msg := wrap.msg

				h := p.computeMsgHash(msg)
				for e := 0; e < 32; e++ {
					wrap.hash[e] = h[e]
				}

				chOut <- wrap
				//fmt.Println("TIMESign ", time.Since(s))

			}
		}(hashInQueues[x], hashOutQueues[x]) //, server[x])

		//server[x] = sha256.NewAvx512Server()

		go func(chIn <-chan *MessageWrapper, chOut chan<- *MessageWrapper) { //, srv *sha256.Avx512Server) {

			for {
				wrap := <-chIn
				//s := time.Now()

				msg := wrap.msg

				hash := wrap.hash

				////s := time.Now()
				auth := &pb.Authenticator{}
				if p.nodeRole[wrap.sendTo[0]] == ROLE_CLIENT && p.usePKToClient == true {
					auth = p.CreateSigForHash(hash, pb.SigType_PKSig, wrap.sendTo[0])
				} else {
					auth = p.CreateSigForHash(hash, pb.SigType_MAC, wrap.sendTo[0])
				}

				//extra sig for the coordinator
				authCoord := &pb.Authenticator{}
				if p.coordPresent {
					authCoord = p.CreateSigForHash(hash, pb.SigType_MAC, p.coordId)
				}

				//fmt.Println("OutSign ", time.Since(s))
				////s = time.Now()

				msg.Auth = make([]*pb.Authenticator, 2)
				msg.Auth[0] = auth
				msg.Auth[1] = authCoord

				dest := make([]int, 1)
				dest[0] = wrap.sendTo[0]
				newWrap := &MessageWrapper{msg: msg, sendTo: dest}

				chOut <- newWrap
				//fmt.Println("TIMESign ", time.Since(s))

			}
		}(signQueues[x], sendQueues[x]) //, server[x])

	}

	for x := 0; x < p.nodeCount; x++ {

		go func(inChan chan *MessageWrapper, cid int) {
			sendBuf := make([]byte, 0)
			pendingBytes := 0
			clone := &MessageWrapper{}
			nextMsg := &MessageWrapper{}
			nextMsg = nil

			for {

				if nextMsg != nil {
					clone = nextMsg
					nextMsg = nil
				} else {
					clone = <-inChan
				}

				raw, err := proto.Marshal(clone.msg)
				if err != nil {
					fmt.Printf("%s\n", err)
				} else {
					lba := make([]byte, 4)
					binary.LittleEndian.PutUint32(lba, uint32(len(raw)))
					sendBuf = append(sendBuf, lba...)
					sendBuf = append(sendBuf, raw...)
					pendingBytes += len(lba) + len(raw)

					select {
					case nextMsg = <-inChan:
						// will do an other pass
						if pendingBytes < 64000 {
							continue
						}
					default:
						nextMsg = nil
					}

					n, err := p.nodeConn[cid].Write(sendBuf)
					sendBuf = make([]byte, 0)
					pendingBytes = 0

					if err != nil {
						fmt.Printf("%s\n", err)
					}
					//fmt.Printf(" Sent %d bytes to %d, seq %d\n", n, cid, clone.msg.MsgId)
					if n == 0 {
						fmt.Printf(" Sent %d bytes\n", n)
					}

				}
			}
		}(peerQueues[x], x)

	}

	go func() {
		hashCnt := 0
		for {
			wrap := <-chIn
			hashInQueues[hashCnt%MAX_SIG_CREATE] <- wrap
			hashCnt++

		}
	}()

	go func() {
		sendCnt := 0
		hashCnt := 0
		for {
			wrap := <-hashOutQueues[hashCnt%MAX_SIG_CREATE]
			hashCnt++

			for _, elem := range wrap.sendTo {

				newWrap := &MessageWrapper{}
				newWrap.msg = p.clonePeerMessageNoAuth(wrap.msg)
				newWrap.sendTo = make([]int, 1)
				newWrap.hash = wrap.hash
				newWrap.sendTo[0] = elem
				signQueues[sendCnt%MAX_SIG_CREATE] <- newWrap
				sendCnt++
			}
		}
	}()

	qCnt := 0
	for {
		clone := <-sendQueues[qCnt%MAX_SIG_CREATE]
		//s := time.Now()
		qCnt++

		peerQueues[clone.sendTo[0]] <- clone

		//fmt.Println("TIMESend ", time.Since(s))

	}
}

func (p Node) computeMsgHash(pm *pb.PeerMessage) [32]byte {
	authSave := pm.Auth
	pm.Auth = nil
	msgBytes, err := proto.Marshal(pm)
	if err != nil {
		fmt.Printf("%s\n", err)
	}

	msgHash := sha256.Sum256(msgBytes)

	pm.Auth = authSave

	return msgHash
}

func (p *Node) CreateSigForHash(hash [32]byte, sigType pb.SigType, toNodeId int) *pb.Authenticator {

	sig := make([]byte, 0)
	dest := toNodeId

	if toNodeId == p.myID {
		return &pb.Authenticator{}
	}

	if sigType == pb.SigType_PKSig {
		sig = p.createPKSig(hash)
		dest = -1
	} else {
		sig = p.createMACSig(int32(toNodeId), hash)
	}

	auth := &pb.Authenticator{FromNodeId: int32(p.myID), ToNodeId: int32(dest), SigType: sigType, Sig: sig}

	return auth

}

//VerifySig Verify the validity of a received message
func (p *Node) VerifySig(pm *pb.PeerMessage) bool {

	if pm.Auth == nil {
		fmt.Println("Message without Auth from ", pm.FromNodeId)
		os.Exit(0)
		return true
	}

	success := false

	if pm.FromNodeId == int32(p.myID) {
		return true
	}

	msgAuth := pm.Auth
	pm.Auth = nil

	msgHash := p.computeMsgHash(pm)

	pm.Auth = msgAuth

	for _, elem := range msgAuth {
		if elem.ToNodeId == int32(p.myID) || elem.ToNodeId == -1 {
			// found a signature for me

			if elem.SigType == pb.SigType_MAC {
				success = p.verifyMACSig(elem.FromNodeId, msgHash, elem.Sig)
			} else if elem.SigType == pb.SigType_PKSig {
				success = p.verifyPKSig(elem.FromNodeId, msgHash, elem.Sig)
			}

			break
		}
	}

	return success
}

func (p *Node) verifyMACSig(fromId int32, hash [32]byte, sig []byte) bool {

	/*if p.MACCipher[fromId] == nil {
		cypher, _ := aes.NewCipher(p.MACKeys[fromId])
		gcm, _ := cipher.NewGCM(cypher)
		p.MACCipher[fromId] = gcm

		nonce := make([]byte, gcm.NonceSize())
		for i := 0; i < gcm.NonceSize(); i++ {
			nonce[i] = byte(p.myID + int(fromId))
		}
		p.MACNonce[fromId] = nonce
	}*/

	nonceSize := p.MACCipher[fromId].NonceSize()

	nonce, ciphertext := sig[:nonceSize], sig[nonceSize:]
	plaintext, err := p.MACCipher[fromId].Open(nil, nonce, ciphertext, nil)

	if err != nil {
		fmt.Printf("%s\n", err)
		return false
	}

	return string(plaintext) == string(hash[:])

}

func (p *Node) createMACSig(toId int32, hash [32]byte) []byte {

	nonceSize := p.MACCipher[toId].NonceSize()

	mac := make([]byte, nonceSize)
	copy(mac, p.MACNonce[toId])
	mac = p.MACCipher[toId].Seal(mac, p.MACNonce[toId], hash[:], nil)

	return mac

}

func (p *Node) verifyPKSig(fromId int32, hash [32]byte, sig []byte) bool {
	if p.PKVerifier[fromId] == nil {
		p.PKVerifier[fromId], _ = cr.LoadPublicKey(p.PubKey[fromId])
	}

	verifier := p.PKVerifier[fromId]
	err := verifier.Unsign(hash[:], sig)

	if err == nil {
		return true
	} else {
		fmt.Printf("%s\n", err)
		return false
	}
}

func (p *Node) createPKSig(hash [32]byte) []byte {
	if p.PKSigner == nil {
		p.PKSigner, _ = cr.LoadPrivateKey(p.myPrivKey)
	}

	sig, _ := p.PKSigner.Sign(hash[:])
	return sig
}

func (p Node) nextStateHash(curHash [32]byte, nextMsg []byte) [32]byte {

	aux := append(nextMsg[:], curHash[:]...)
	hash := sha256.Sum256(aux)

	return hash

}

func (p Node) clonePeerMessage(req *pb.PeerMessage) *pb.PeerMessage {
	reqAClone := make([]byte, len(req.AttachedData))
	copy(reqAClone, req.AttachedData)
	retv := &pb.PeerMessage{FromNodeId: req.FromNodeId, MsgId: req.MsgId, EpochId: req.EpochId, Type: req.Type, AttachedData: reqAClone, Auth: req.Auth}
	return retv
}

func (p Node) clonePeerMessageNoAuth(req *pb.PeerMessage) *pb.PeerMessage {
	reqAClone := make([]byte, len(req.AttachedData))
	copy(reqAClone, req.AttachedData)
	retv := &pb.PeerMessage{FromNodeId: req.FromNodeId, MsgId: req.MsgId, EpochId: req.EpochId, Type: req.Type, AttachedData: reqAClone}
	return retv
}

func (p Node) checkAndExtract(req *pb.PeerMessage) (bool, PeerState) {
	cMsg := &pb.CertificateMessage{}
	err := proto.Unmarshal(req.AttachedData, cMsg)
	if err != nil {
		fmt.Printf("%s\n", err)
		return false, PeerState{}
	} else if cMsg.Initialized {

		slice := cMsg.Certificates[0:p.peerCount]
		result, vMsg := p.enoughValidAndMatching(slice)
		if result {
			dCheckpoint := make([]byte, len(vMsg.AttachedData))
			copy(dCheckpoint, vMsg.AttachedData)
			nSequence := int(vMsg.MsgId)
			eMaxPrepared := vMsg.EpochId
			for x := 1; x < int(cMsg.CntPrepared+1); x++ {
				//fmt.Printf("length slice %d, next slice %d:%d\n", len(cMsg.Certificates), x*p.peerCount, x*p.peerCount+(p.peerCount))
				slice = cMsg.Certificates[x*p.peerCount : x*p.peerCount+(p.peerCount)]
				resultP, vMsgP := p.enoughValidAndMatching(slice)
				//fmt.Printf("result %d, current sequence %d, return sequence %d\n", resultP, int(vMsgP.MsgId), nSequence)
				if resultP && int(vMsgP.MsgId) == nSequence+1 {
					nSequence++
					if eMaxPrepared < vMsgP.EpochId {
						eMaxPrepared = vMsgP.EpochId
					}
				} else {
					return false, PeerState{}
				}
			}
			msgBytes, err := proto.Marshal(cMsg)
			if err != nil {
				fmt.Printf("%s\n", err)
			}
			msgHash := sha256.Sum256(msgBytes)
			return true, PeerState{int(req.FromNodeId), true, nSequence, dCheckpoint, msgHash[:], int(eMaxPrepared)}
		} else {
			return false, PeerState{}
		}
		return false, PeerState{}
	} else {
		return true, PeerState{initialized: false}
	}
}

func (p Node) enoughValidAndMatching(messages []*pb.PeerMessage) (bool, *pb.PeerMessage) {
	success := 0
	var e, n int
	var d []byte
	vMsg := &pb.PeerMessage{}
	for _, msg := range messages {
		if p.VerifySig(msg) {
			if success == 0 {
				e = int(msg.EpochId)
				n = int(msg.MsgId)
				d = make([]byte, len(msg.AttachedData))
				copy(d, msg.AttachedData)
				vMsg = msg
				success++
			} else {
				if e == int(msg.EpochId) && n == int(msg.MsgId) && bytes.Compare(d, msg.AttachedData) == 0 {
					success++
				} else {
					return false, nil
				}
			}
		}
	}
	if success >= p.f+1 {
		return true, vMsg
	} else {
		return false, nil
	}
}

func (p Node) isOperational(states []PeerState, f int) bool {
	initialized := 0
	for _, state := range states {
		if state.initialized {
			if state.maxEpoch >= p.myEpoch {
				return true
			} else {
				initialized++
			}
		}
	}
	if initialized >= f {
		return true
	} else {
		return false
	}
}

func (p Node) computeMembership(states []PeerState) ([]int, int, []byte, []byte) {
	max := 0
	leader := -1
	var ds []byte
	var dp []byte
	for _, state := range states {
		if state.initialized {
			if state.maxPrepared >= max {
				max = state.maxPrepared
				leader = state.nodeId
				ds = state.digestState
				dp = state.digestCertificates
			}
		}
	}
	followers := make([]int, p.peerCount-1)
	c := 0
	for x := 0; x < p.peerCount; x++ {
		if x != leader {
			followers[c] = x
			c++
		}
	}
	return followers, leader, ds, dp
}
