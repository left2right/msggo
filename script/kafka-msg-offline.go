package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"
	"os/signal"
	"strconv"
	"strings"
	"sync"
        "encoding/json"
	"hash/crc32"

	"github.com/Shopify/sarama"
)

var (
	brokerList = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The comma separated list of brokers in the Kafka cluster")
	topic      = flag.String("topic", "", "REQUIRED: the topic to consume")
	partitions = flag.String("partitions", "all", "The partitions to consume, can be 'all' or comma-separated numbers")
	offset     = flag.String("offset", "newest", "The offset to start with. Can be `oldest`, `newest`")
	verbose    = flag.Bool("verbose", false, "Whether to turn on sarama logging")
	bufferSize = flag.Int("buffer-size", 256, "The buffer size of the message channel.")
	timeBegin  = flag.String("time-begin", "2016-01-02 15:04:05", "Time where to begin,")
	timeEnd    = flag.String("time-end", "2016-01-02 16:04:05", "Time where to end,")
	slots      = flag.String("slots", "0,1,2", "Slots to process,")
	outfile      = flag.String("output-file", "msgIdNoAckList.data", "Output file, key and msg id write to,")

	logger = log.New(os.Stderr, "", log.LstdFlags)
)

func main() {
	flag.Parse()

	if *brokerList == "" {
		printUsageErrorAndExit("You have to provide -brokers as a comma-separated list, or set the KAFKA_PEERS environment variable.")
	}

	if *topic == "" {
		printUsageErrorAndExit("-topic is required")
	}

	if *verbose {
		sarama.Logger = logger
	}

	if *timeEnd == "" {
		printUsageErrorAndExit("-time-end is required")
	}

	if *slots == "" {
		printUsageErrorAndExit("-slots is required")
	}


	var initialOffset int64
	switch *offset {
	case "oldest":
		initialOffset = sarama.OffsetOldest
	case "newest":
		initialOffset = sarama.OffsetNewest
	default:
		printUsageErrorAndExit("-offset should be `oldest` or `newest`")
	}

	c, err := sarama.NewConsumer(strings.Split(*brokerList, ","), nil)
	if err != nil {
		printErrorAndExit(69, "Failed to start consumer: %s", err)
	}

	partitionList, err := getPartitions(c)
	if err != nil {
		printErrorAndExit(69, "Failed to get the list of partitions: %s", err)
	}

	var (
		messages = make(chan *sarama.ConsumerMessage, *bufferSize)
		closing  = make(chan struct{})
		wg       sync.WaitGroup
	)

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt)
		<-signals
		logger.Println("Initiating shutdown of consumer...")
		close(closing)
	}()

	for _, partition := range partitionList {
		pc, err := c.ConsumePartition(*topic, partition, initialOffset)
		if err != nil {
			printErrorAndExit(69, "Failed to start consumer for partition %d: %s", partition, err)
		}

		go func(pc sarama.PartitionConsumer) {
			<-closing
			pc.AsyncClose()
		}(pc)

		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			for message := range pc.Messages() {
				messages <- message
			}
		}(pc)
	}

        //write output to file
        fout, err := os.Create(*outfile)
        defer fout.Close()
        if err != nil {
		printErrorAndExit(69, "Failed to create output file: %s", err)
        }
       
        var msgJson map[string]interface{}
	tpBegin := timeToTimeStamp(*timeBegin)
	tpEnd := timeToTimeStamp(*timeEnd)
	slotList := slotsToInts(*slots)
    	fmt.Println("Begin to consume!")
	go func() {
		for msg := range messages {
			//fmt.Printf("%s\n\n", string(msg.Value))
                        json.Unmarshal([]byte(string(msg.Value)), &msgJson)
			key := "index:unread:"+msgJson["from"].(string)+":"+msgJson["to"].(string)
			if (tpBegin < int64(msgJson["timestamp"].(float64))) && (int64(msgJson["timestamp"].(float64)) < tpEnd) &&(keyInGroup([]byte(key),slotList)) {
				fmt.Fprintf(fout,"%s %s\n", msgJson["to"], msgJson["msg_id"])
			} else if int64(msgJson["timestamp"].(float64)) > tpEnd {
				fmt.Println("Finish consume!")
				os.Exit(1)
			}
		}
	}()

	wg.Wait()
	logger.Println("Done consuming topic", *topic)
	close(messages)

	if err := c.Close(); err != nil {
		logger.Println("Failed to close consumer: ", err)
	}
}

func slotsToInts(slotsStr string) []int {
	slots := []int{}
	for _, i := range strings.Split(strings.TrimSpace(slotsStr), ",") {
		j, err := strconv.Atoi(i)
		if err != nil {
			panic(err)
		}
		slots = append(slots, j)
	}
	return slots
}

func timeToTimeStamp(tString string) int64 {
	the_time, err := time.ParseInLocation("2006-01-02 15:04:05", tString, time.Local)
	if err != nil {
		os.Exit(1)
	}
        timestamp := the_time.Unix()*1000
    	//fmt.Println(timestamp)
	return timestamp
}

func keyInGroup(key []byte, slots []int) bool {
	keySlot := int(crc32.ChecksumIEEE(key)%1024)
    	//fmt.Println(keySlot)
	for _, slot := range slots {
		if keySlot == slot {
			return true
		}
	}
	return false
}

func getPartitions(c sarama.Consumer) ([]int32, error) {
	if *partitions == "all" {
		return c.Partitions(*topic)
	}

	tmp := strings.Split(*partitions, ",")
	var pList []int32
	for i := range tmp {
		val, err := strconv.ParseInt(tmp[i], 10, 32)
		if err != nil {
			return nil, err
		}
		pList = append(pList, int32(val))
	}

	return pList, nil
}

func printErrorAndExit(code int, format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	os.Exit(code)
}

func printUsageErrorAndExit(format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Available command line options:")
	flag.PrintDefaults()
	os.Exit(64)
}
