# kafka-msg-offline
### Build

go build kafka-msg-offline.go

### Usage
	Available command line options:
  
	-brokers string
        
		The comma separated list of brokers in the Kafka cluster
  
	-buffer-size int
       
		The buffer size of the message channel. (default 256)
  
	-offset oldest
        
		The offset to start with. Can be oldest, `newest` (default "newest")
  
	-output-file string
        
		Output file, key and msg id write to, (default "msgIdNoAckList.data")
  
	-partitions string
        
		The partitions to consume, can be 'all' or comma-separated numbers (default "all")
  
	-slots string
        
		Slots to process, (default "0,1,2")
  
	-time-begin string
        
		Time where to begin, (default "2016-01-02 15:04:05")
  
	-time-end string
        
		Time where to end, (default "2016-01-02 16:04:05")
  
	-topic string
        
		REQUIRED: the topic to consume
  
	-verbose
        
		Whether to turn on sarama logging

### example

./kafka-msg-offline -topic=ejabberd-chat-offlines -brokers=localhost:9092 -offset oldest -time-begin "2016-01-02 15:04:05" -time-end "2016-05-16 14:06:05" -slots 1,3,100,420,431 -output-file msgNoAckOutput.data


