package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/kafka"
	"golang.org/x/sync/semaphore"
)

func Die(msg string, args ...interface{}) {
	formatted := fmt.Sprintf(msg, args...)
	log.Error(formatted)
	os.Exit(1)
}

func Chk(err error, msg string, args ...interface{}) {
	if err != nil {
		Die(msg, args...)
	}
}

var (
	debug        = flag.Bool("debug", false, "Enable verbose logging")
	trace        = flag.Bool("trace", false, "Enable super-verbose (franz-go internals)")
	brokers      = flag.String("brokers", "localhost:9092", "comma delimited list of brokers")
	topic        = flag.String("topic", "", "topic to produce to or consume from")
	username     = flag.String("username", "", "SASL username")
	password     = flag.String("password", "", "SASL password")
	mSize        = flag.Int("msg_size", 16384, "Size of messages to produce")
	pCount       = flag.Int("produce_msgs", 1000, "Number of messages to produce")
	cCount       = flag.Int("rand_read_msgs", 10, "Number of validation reads to do")
	seqRead      = flag.Bool("seq_read", true, "Whether to do sequential read validation")
	parallelRead = flag.Int("parallel", 1, "How many readers to run in parallel")
)

type OffsetRange struct {
	Lower int64 // Inclusive
	Upper int64 // Exclusive
}

type OffsetRanges struct {
	Ranges []OffsetRange
}

func (ors *OffsetRanges) Insert(o int64) {
	// Normal case: this is the next offset after the current range in flight

	if len(ors.Ranges) == 0 {
		ors.Ranges = append(ors.Ranges, OffsetRange{Lower: o, Upper: o + 1})
		return
	}

	last := &ors.Ranges[len(ors.Ranges)-1]
	if o >= last.Lower && o == last.Upper {
		last.Upper += 1
		return
	} else {
		if o < last.Upper {
			// TODO: more flexible structure for out of order inserts, at the moment
			// we rely on franz-go callbacks being invoked in order.
			Die("Out of order offset %d", o)
		} else {
			ors.Ranges = append(ors.Ranges, OffsetRange{Lower: o, Upper: o + 1})
		}
	}
}

func (ors *OffsetRanges) Contains(o int64) bool {
	for _, r := range ors.Ranges {
		if o >= r.Lower && o < r.Upper {
			return true
		}
	}

	return false
}

type TopicOffsetRanges struct {
	PartitionRanges []OffsetRanges
}

func (tors *TopicOffsetRanges) Insert(p int32, o int64) {
	tors.PartitionRanges[p].Insert(o)
}

func (tors *TopicOffsetRanges) Contains(p int32, o int64) bool {
	return tors.PartitionRanges[p].Contains(o)
}

func topicOffsetRangeFile() string {
	return fmt.Sprintf("valid_offsets_%s.json", *topic)
}

func (tors *TopicOffsetRanges) Store() error {
	log.Infof("TopicOffsetRanges::Storing %s...", topicOffsetRangeFile())
	data, err := json.Marshal(tors)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(topicOffsetRangeFile(), data, 0644)
	if err != nil {
		return err
	}

	for p, or := range tors.PartitionRanges {
		log.Debugf("TopicOffsetRanges::Store: %d %d", p, len(or.Ranges))
	}

	return nil
}

func NewTopicOffsetRanges(nPartitions int32) TopicOffsetRanges {
	prs := make([]OffsetRanges, nPartitions)
	for _, or := range prs {
		or.Ranges = make([]OffsetRange, 0)
	}
	return TopicOffsetRanges{
		PartitionRanges: prs,
	}
}

func LoadTopicOffsetRanges(nPartitions int32) TopicOffsetRanges {
	data, err := ioutil.ReadFile(topicOffsetRangeFile())
	if err != nil {
		// Pass, assume it's not existing yet
		return NewTopicOffsetRanges(nPartitions)
	} else {
		var tors TopicOffsetRanges
		if len(data) > 0 {
			err = json.Unmarshal(data, &tors)
			Chk(err, "Bad JSON %v", err)
		}

		if int32(len(tors.PartitionRanges)) > nPartitions {
			Die("More partitions in valid_offsets file than in topic!")
		} else if len(tors.PartitionRanges) < int(nPartitions) {
			// Creating new partitions is allowed
			blanks := make([]OffsetRanges, nPartitions-int32(len(tors.PartitionRanges)))
			tors.PartitionRanges = append(tors.PartitionRanges, blanks...)
		}

		return tors
	}
}

func sequentialRead(nPartitions int32) {
	client := newClient(nil)
	hwm := getOffsets(client, nPartitions, -1)
	lwm := make([]int64, nPartitions)

	for {
		var err error
		lwm, err = sequentialReadInner(nPartitions, lwm, hwm)
		if err != nil {
			log.Warnf("Restarting reader for error %v", err)
			// Loop around
		} else {
			return
		}
	}
}

func sequentialReadInner(nPartitions int32, startAt []int64, upTo []int64) ([]int64, error) {
	log.Infof("Sequential read...")

	offsets := make(map[string]map[int32]kgo.Offset)
	partOffsets := make(map[int32]kgo.Offset, nPartitions)
	complete := make([]bool, nPartitions)
	for i, o := range startAt {
		partOffsets[int32(i)] = kgo.NewOffset().At(o)
		log.Infof("Sequential start offset %s/%d %d...", *topic, i, partOffsets[int32(i)])
		if o == upTo[i] {
			complete[i] = true
		}
	}
	offsets[*topic] = partOffsets

	validRanges := LoadTopicOffsetRanges(nPartitions)

	opts := []kgo.Opt{
		kgo.ConsumePartitions(offsets),
	}
	client := newClient(opts)

	last_read := make([]int64, nPartitions)

	for {
		fetches := client.PollFetches(context.Background())

		var r_err error
		fetches.EachError(func(t string, p int32, err error) {
			log.Debugf("Sequential fetch %s/%d e=%v...", t, p, err)
			r_err = err
		})

		if r_err != nil {
			return last_read, r_err
		}

		fetches.EachRecord(func(r *kgo.Record) {
			log.Debugf("Sequential read %s/%d o=%d...", *topic, r.Partition, r.Offset)
			if r.Offset > last_read[r.Partition] {
				last_read[r.Partition] = r.Offset
			}

			if r.Offset >= upTo[r.Partition]-1 {
				complete[r.Partition] = true
			}

			validateRecord(r, &validRanges)
		})

		any_incomplete := false
		for _, c := range complete {
			if !c {
				any_incomplete = true
			}

		}

		if !any_incomplete {
			break
		}
	}

	return last_read, nil
}

func validateRecord(r *kgo.Record, validRanges *TopicOffsetRanges) {
	expect_key := fmt.Sprintf("%06d.%018d", 0, r.Offset)
	log.Debugf("Consumed %s on p=%d at o=%d", r.Key, r.Partition, r.Offset)
	if expect_key != string(r.Key) {
		shouldBeValid := validRanges.Contains(r.Partition, r.Offset)

		if shouldBeValid {
			Die("Bad read at offset %d on partition %s/%d.  Expect '%s', found '%s'", r.Offset, *topic, r.Partition, expect_key, r.Key)
		} else {
			log.Infof("Ignoring read validation at offset outside valid range %s/%d %d", *topic, r.Partition, r.Offset)
		}
	} else {
		log.Debugf("Read OK (%s) on p=%d at o=%d", r.Key, r.Partition, r.Offset)

	}
}

func randomRead(nPartitions int32) {

	// Basic client to read offsets
	client := newClient(make([]kgo.Opt, 0))
	endOffsets := getOffsets(client, nPartitions, -1)
	client.Close()
	client = newClient(make([]kgo.Opt, 0))
	startOffsets := getOffsets(client, nPartitions, -2)
	client.Close()
	runtime.GC()

	// FIXME: Weird franz-go bug?  When I use getOffsets twice
	// on the same client, the second one gets not_leader errors
	// for a couple of partitions (but not on all topics!  I just
	// had one topic that was in this state)

	validRanges := LoadTopicOffsetRanges(nPartitions)

	// Select a partition and location
	log.Infof("Reading %d random offsets", *cCount)
	for i := 0; i < *cCount; i++ {
		p := rand.Int31n(nPartitions)
		pStart := startOffsets[p]
		pEnd := endOffsets[p]

		if pStart == pEnd {
			log.Warnf("Partition %d is empty, skipping read", p)
			continue
		}
		o := rand.Int63n(pEnd-pStart) + pStart
		offset := kgo.NewOffset().At(o)
		log.Debugf("Read partition %d (%d-%d) at offset %d", p, pStart, pEnd, offset)

		// Construct a map of topic->partition->offset to seek our new client to the right place
		offsets := make(map[string]map[int32]kgo.Offset)
		partOffsets := make(map[int32]kgo.Offset, 1)
		partOffsets[p] = offset
		offsets[*topic] = partOffsets

		// Fully-baked client for actual consume
		opts := []kgo.Opt{
			kgo.ConsumeResetOffset(offset),
			kgo.ConsumeTopics(*topic),
			kgo.ConsumePartitions(offsets),
		}

		// FIXME(franz-go) - if you pass ConsumeResetOffset AND ConsumePartitions or ConsumeTopics, it accepts
		// both but you don't get what you expect.

		client = newClient(opts)

		// Read one record
		fetches := client.PollRecords(context.Background(), 1)
		fetches.EachRecord(func(r *kgo.Record) {
			if r.Partition != p {
				Die("Wrong partition %d in read at offset %d on partition %s/%d", r.Partition, r.Offset, *topic, p)
			}
			validateRecord(r, &validRanges)
		})
		fetches = nil

		client.Flush(context.Background())
		client.Close()
		runtime.GC()

		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		log.Debugf("After close bytes: %d goroutines: %d", m.Alloc, runtime.NumGoroutine())
		if false {
			prof_file, err := os.OpenFile("out.bin", os.O_CREATE|os.O_RDWR, 0755)
			Chk(err, "opening heap file %v", err)
			pprof.WriteHeapProfile(prof_file)
		}

	}

}

func newRecord(producerId int, sequence int64) *kgo.Record {
	var key bytes.Buffer
	fmt.Fprintf(&key, "%06d.%018d", producerId, sequence)

	payload := make([]byte, *mSize)

	var r *kgo.Record
	r = kgo.KeySliceRecord(key.Bytes(), payload)
	return r
}

// Try to get offsets, with a retry loop in case any partitions are not
// in a position to respond.  This is useful to avoid terminating if e.g.
// the cluster is subject to failure injection while workload runs.
func getOffsets(client *kgo.Client, nPartitions int32, t int64) []int64 {
	wait_t := 2 * time.Second
	for {
		result, err := getOffsetsInner(client, nPartitions, t)
		if err != nil {
			log.Warnf("Retrying getOffsets in %v", wait_t)
			time.Sleep(wait_t)
		} else {
			return result
		}

	}
}

func getOffsetsInner(client *kgo.Client, nPartitions int32, t int64) ([]int64, error) {
	log.Infof("Loading offsets for topic %s t=%d...", *topic, t)
	pOffsets := make([]int64, nPartitions)

	req := kmsg.NewPtrListOffsetsRequest()
	req.ReplicaID = -1
	reqTopic := kmsg.NewListOffsetsRequestTopic()
	reqTopic.Topic = *topic
	for i := 0; i < int(nPartitions); i++ {
		part := kmsg.NewListOffsetsRequestTopicPartition()
		part.Partition = int32(i)
		part.Timestamp = t
		reqTopic.Partitions = append(reqTopic.Partitions, part)
	}

	req.Topics = append(req.Topics, reqTopic)

	seenPartitions := int32(0)
	// FIXME: franz-go fails in weird ways if RequestSharded isn't used
	shards := client.RequestSharded(context.Background(), req)
	var r_err error
	allFailed := kafka.EachShard(req, shards, func(shard kgo.ResponseShard) {
		if shard.Err != nil {
			r_err = shard.Err
			return
		}
		resp := shard.Resp.(*kmsg.ListOffsetsResponse)
		for _, partition := range resp.Topics[0].Partitions {
			if partition.ErrorCode != 0 {
				log.Warnf("error fetching %s/%d metadata: %v", *topic, partition.Partition, kerr.ErrorForCode(partition.ErrorCode))
				r_err = kerr.ErrorForCode(partition.ErrorCode)
			}
			pOffsets[partition.Partition] = partition.Offset
			seenPartitions += 1
			log.Debugf("Partition %d offset %d", partition.Partition, pOffsets[partition.Partition])
		}
	})

	if allFailed {
		return nil, errors.New("All offset requests failed")
	}

	if seenPartitions < nPartitions {
		// The results may be partial, simply omitting some partitions while not
		// raising any error.  We transform this into an error to avoid wrongly
		// returning a 0 offset for any missing partitions
		return nil, errors.New("Didn't get data for all partitions")
	}

	return pOffsets, r_err
}

func produce(nPartitions int32) {
	n := int64(*pCount)
	for {
		n_produced, bad_offsets := produceInner(n, nPartitions)
		n = n - n_produced

		if len(bad_offsets) > 0 {
			log.Infof("Produce stopped early, %d still to do", n)
		}

		if n <= 0 {
			return
		}
	}
}

type BadOffset struct {
	P int32
	O int64
}

func produceInner(n int64, nPartitions int32) (int64, []BadOffset) {
	opts := []kgo.Opt{
		kgo.DefaultProduceTopic(*topic),
		kgo.MaxBufferedRecords(1024),
		kgo.ProducerBatchMaxBytes(1024 * 1024),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	}
	client := newClient(opts)

	validOffsets := LoadTopicOffsetRanges(nPartitions)

	nextOffset := getOffsets(client, nPartitions, -1)

	for i, o := range nextOffset {
		log.Infof("Produce start offset %s/%d %d...", *topic, i, o)
	}

	var wg sync.WaitGroup

	errored := false
	produced := int64(0)

	// Channel must be >= concurrency
	bad_offsets := make(chan BadOffset, 16384)
	concurrent := semaphore.NewWeighted(4096)

	log.Infof("Producing %d messages (%d bytes)", n, *mSize)

	storeEveryN := 10000

	for i := int64(0); i < n && len(bad_offsets) == 0; i = i + 1 {
		concurrent.Acquire(context.Background(), 1)
		produced += 1
		var p = rand.Int31n(nPartitions)

		expect_offset := nextOffset[p]
		nextOffset[p] += 1

		r := newRecord(0, expect_offset)
		r.Partition = p
		wg.Add(1)

		log.Debugf("Writing partition %d at %d", r.Partition, nextOffset[p])
		handler := func(r *kgo.Record, err error) {
			concurrent.Release(1)
			Chk(err, "Produce failed!")
			if expect_offset != r.Offset {
				log.Warnf("Produced at unexpected offset %d (expected %d) on partition %d", r.Offset, expect_offset, r.Partition)
				bad_offsets <- BadOffset{r.Partition, r.Offset}
				errored = true
				log.Debugf("errored = %b", errored)
			} else {
				validOffsets.Insert(r.Partition, r.Offset)
				log.Debugf("Wrote partition %d at %d", r.Partition, r.Offset)
			}
			wg.Done()
		}
		client.Produce(context.Background(), r, handler)

		// Not strictly necessary, but useful if a long running producer gets killed
		// before finishing
		if i%int64(storeEveryN) == 0 && i != 0 {
			err := validOffsets.Store()
			Chk(err, "Error writing interim results: %v", err)
		}
	}

	log.Info("Waiting...")
	wg.Wait()
	log.Info("Waited.")
	wg.Wait()
	close(bad_offsets)

	err := validOffsets.Store()
	Chk(err, "Error writing interim results: %v", err)

	if errored {
		log.Warnf("%d bad offsets", len(bad_offsets))
		var r []BadOffset
		for o := range bad_offsets {
			r = append(r, o)
		}
		if len(r) == 0 {
			Die("No bad offsets but errored?")
		}
		successful_produced := produced - int64(len(r))
		return successful_produced, r
	} else {
		wg.Wait()
		return produced, nil
	}
}

func newClient(opts []kgo.Opt) *kgo.Client {
	// Disable auth if username not given
	if len(*username) > 0 {
		auth_mech := scram.Auth{
			User: *username,
			Pass: *password,
		}
		auth := auth_mech.AsSha256Mechanism()
		opts = append(opts,
			kgo.SASL(auth))
	}

	opts = append(opts,
		kgo.SeedBrokers(strings.Split(*brokers, ",")...))

	if *trace {
		opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelDebug, nil)))
	}

	client, err := kgo.NewClient(opts...)
	Chk(err, "Error creating kafka client")
	return client
}

func main() {
	flag.Parse()

	if *debug || *trace {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	log.Info("Getting topic metadata...")
	client := newClient(make([]kgo.Opt, 0))

	var t kmsg.MetadataResponseTopic
	{
		req := kmsg.NewPtrMetadataRequest()
		reqTopic := kmsg.NewMetadataRequestTopic()
		reqTopic.Topic = kmsg.StringPtr(*topic)
		req.Topics = append(req.Topics, reqTopic)

		resp, err := req.RequestWith(context.Background(), client)
		Chk(err, "unable to request topic metadata: %v", err)
		if len(resp.Topics) != 1 {
			Die("metadata response returned %d topics when we asked for 1", len(resp.Topics))
		}
		t = resp.Topics[0]
		if t.ErrorCode != 0 {
			Die("Error %s getting topic metadata", kerr.ErrorForCode(t.ErrorCode))
		}
	}

	nPartitions := int32(len(t.Partitions))
	log.Debugf("Targeting topic %s with %d partitions", *topic, nPartitions)

	if *pCount > 0 {
		produce(nPartitions)
	}

	if *parallelRead <= 1 {
		if *seqRead {
			sequentialRead(nPartitions)
		}

		if *cCount > 0 {
			randomRead(nPartitions)
		}
	} else {
		var wg sync.WaitGroup
		if *seqRead {
			wg.Add(1)
			go func() {
				sequentialRead(nPartitions)
				wg.Done()
			}()
		}

		parallelRandoms := *parallelRead
		if *seqRead {
			parallelRandoms -= 1
		}

		if *cCount > 0 {
			for i := 0; i < parallelRandoms; i++ {
				wg.Add(1)
				go func() {
					randomRead(nPartitions)
					wg.Done()
				}()
			}
		}

		wg.Wait()

	}

}
