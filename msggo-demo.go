package main

import (
    "flag"
    "fmt"
    "log"
    "os"
    "os/signal"
    "strconv"
    "strings"
    "sync"
    "time"
    "encoding/json"
    "net/http"

    "github.com/Shopify/sarama"
    "github.com/seefan/gossdb"
    "github.com/labstack/echo"
    "github.com/labstack/echo/engine/standard"
)

var (
    brokerList = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The comma separated list of brokers in the Kafka cluster")
    topic      = flag.String("topic", "", "REQUIRED: the topic to consume")
    partitions = flag.String("partitions", "all", "The partitions to consume, can be 'all' or comma-separated numbers")
    offset     = flag.String("offset", "newest", "The offset to start with. Can be `oldest`, `newest`")
    verbose    = flag.Bool("verbose", false, "Whether to turn on sarama logging")
    bufferSize = flag.Int("buffer-size", 256, "The buffer size of the message channel.")
    //ssdbAddr   = flag.String("ssdb", "127.0.0.1:8888", "SSDB addr")

    logger = log.New(os.Stderr, "", log.LstdFlags)
    timeStamp string
)

func main() {
    flag.Parse()
    //ssdb
    ssdbIP := "127.0.0.1"
    ssdbPort := 8888

    pool, err := gossdb.NewPool(&gossdb.Config{
        Host: ssdbIP,
        Port: ssdbPort,
    })
    if err != nil {
        fmt.Errorf("error new pool %v", err)
        return
    }
    gossdb.Encoding = true

    ssdbClient, err := pool.NewClient()
    if err != nil {
        fmt.Errorf("new client err=%v", err)
        return
    }
    defer ssdbClient.Close()

   //echo
    e := echo.New()
    e.GET("/", func(c echo.Context) error {
        return c.String(http.StatusOK, "Hello, World!")
    })
    //curl http://localhost:1323/outgoing
    e.GET("/outgoing", func(c echo.Context) error {
        sOut,_ := ssdbClient.Qrange("im:msggo:easemob-demo#chatdemoui_weiying1@easemob.com/mobile:1463391780:ejabberd-chat-messages", 0, 10)
        fmt.Println(sOut)
        return c.JSON(http.StatusOK, sOut)
    })
    //curl http://localhost:1323/outgoing?easemob-demo#chatdemoui_weiying1
    e.GET("/outgoing/:user", func(c echo.Context) error {
        user := strings.TrimSpace(c.Param("user"))
        fmt.Println(user)
        sOut,_ := ssdbClient.Qrange("im:msggo:"+user+":"+"@easemob.com/mobile:1463391780:ejabberd-chat-messages", 0, 10)
        return c.JSON(http.StatusOK, sOut)
    })
    e.Run(standard.New(":1323"))

    //kafka
    if *brokerList == "" {
        printUsageErrorAndExit("You have to provide -brokers as a comma-separated list, or set the KAFKA_PEERS environment variable.")
    }

    if *topic == "" {
        printUsageErrorAndExit("-topic is required")
    }

    if *verbose {
        sarama.Logger = logger
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

    kafkaConsumer, err := sarama.NewConsumer(strings.Split(*brokerList, ","), nil)
    if err != nil {
        printErrorAndExit(69, "Failed to start consumer: %s", err)
    }

    partitionList, err := getPartitions(kafkaConsumer)
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
        pc, err := kafkaConsumer.ConsumePartition(*topic, partition, initialOffset)
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

    //timer
    setTimeStamp()
    timer := time.NewTimer(time.Second * time.Duration(60))
    go func() {
        <-timer.C
        setTimeStamp()
    }()

    var msgJSON map[string]interface{}
    go func() {
        for msg := range messages {
            fmt.Printf("Partition:\t%d\n", msg.Partition)
            fmt.Printf("Offset:\t%d\n", msg.Offset)
            fmt.Printf("Key:\t%s\n", string(msg.Key))
            fmt.Printf("Value:\t%s\n", string(msg.Value))
         
            json.Unmarshal([]byte(string(msg.Value)), &msgJSON)
            key := "im:msggo:"+msg.Topic+":"+timeStamp+":"+msgJSON["from"].(string)
            ssdbClient.Qpush(key, string(msg.Value))
            fmt.Println()
        }
    }()

    wg.Wait()
    logger.Println("Done consuming topic", *topic)
    close(messages)

    if err := kafkaConsumer.Close(); err != nil {
        logger.Println("Failed to close consumer: ", err)
    }
}

func setTimeStamp() {
    timeNow := time.Now()
    year, month, day := timeNow.Date()
    hour := timeNow.Hour()
    minute := timeNow.Minute()
    timeStamp = strconv.FormatInt(time.Date(year, month, day, hour, minute, 0, 0, time.Local).Unix(), 10)
    print(timeStamp)
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

