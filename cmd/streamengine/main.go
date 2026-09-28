package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"

	"github.com/highcumontoa/event-time-stream-engine-go/internal/engine"
)

// demoFaultSink 在真实 FileSink 外包一层可控故障，用于本地演练写出失败重试/重放。
type demoFaultSink struct {
	inner    *engine.FileSink
	failNext int
	logger   *log.Logger
}

func (s *demoFaultSink) Commit(cpID uint64, results []engine.WindowResult, lates []engine.LateRecord) error {
	if s.failNext > 0 {
		s.failNext--
		s.logger.Printf("[demo] injected sink write failure at checkpoint %d (retained, will retry)", cpID)
		return fmt.Errorf("injected sink failure at cp=%d", cpID)
	}
	return s.inner.Commit(cpID, results, lates)
}

func (s *demoFaultSink) TruncateTo(cpID uint64) error { return s.inner.TruncateTo(cpID) }

// 本地演示：生成包含乱序、重复与迟到事件的输入文件，
// 运行引擎进行事件时间窗口聚合，并打印结果与指标。
func main() {
	dir := flag.String("dir", "demo-data", "工作目录（输入/输出/检查点）")
	nEvents := flag.Int("events", 2000, "生成事件数量")
	crash := flag.Bool("crash", false, "模拟中途崩溃并重启恢复")
	cpGenerations := flag.Int("cp-generations", 3, "保留的检查点代数上限")
	restoreID := flag.Uint64("restore-id", 0, "从指定代数检查点恢复（0=最新一代）")
	sinkFail := flag.Int("sink-fail", 0, "写出阶段前 N 次提交注入失败（演练重试/重放）")
	flag.Parse()

	if err := os.MkdirAll(*dir, 0o755); err != nil {
		log.Fatal(err)
	}
	inputPath := filepath.Join(*dir, "input.jsonl")
	resultsPath := filepath.Join(*dir, "results.jsonl")
	latePath := filepath.Join(*dir, "late.jsonl")
	cpPath := filepath.Join(*dir, "checkpoint.json")

	if _, err := os.Stat(inputPath); os.IsNotExist(err) {
		if err := genInput(inputPath, *nEvents); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("generated input: %s (%d events)\n", inputPath, *nEvents)
	}

	logger := log.New(os.Stderr, "[engine] ", log.Lmsgprefix)
	cfg := engine.Config{
		WindowSizeMs:        60_000, // 1 分钟窗口
		AllowedLatenessMs:   30_000, // 允许迟到 30 秒
		NumShards:           4,
		Workers:             4,
		QueueCapacity:       32,
		CheckpointEvery:     200,
		Logger:              logger,
		MaxCheckpoints:      *cpGenerations,
		RestoreCheckpointID: *restoreID,
	}

	run := func(ctx context.Context) error {
		src := engine.NewFileSource(inputPath)
		defer src.Close()
		var sink engine.Sink = engine.NewFileSink(resultsPath, latePath)
		if *sinkFail > 0 {
			sink = &demoFaultSink{inner: engine.NewFileSink(resultsPath, latePath), failNext: *sinkFail, logger: logger}
		}
		store := engine.NewCheckpointStore(cpPath)
		e := engine.NewEngine(cfg, src, sink, store)
		if err := e.Run(ctx); err != nil {
			return err
		}
		st := e.Stats()
		fmt.Printf("stats: processed=%d duplicates=%d late=%d backpressure_blocks=%d checkpoints=%d\n"+
			"       checkpoint_evictions=%d sink_retries=%d replay_results=%d replay_lates=%d "+
			"pending_limit_hits=%d sink_fatal=%d\n",
			st.Processed, st.Duplicates, st.Late, st.BackpressureBlocks, st.Checkpoints,
			st.CheckpointEvictions, st.SinkRetries, st.SinkReplayResults, st.SinkReplayLates,
			st.PendingLimitHits, st.SinkFatal)
		if gens, err := store.List(); err == nil {
			fmt.Printf("retained checkpoint generations (newest-first): %v\n", gens)
		}
		return nil
	}

	if *crash {
		// 第一次运行：处理一部分后取消，模拟崩溃。
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			// 简单起见：让引擎先跑一小段时间再“崩溃”。
			for i := 0; i < 50; i++ {
				if ctx.Err() != nil {
					return
				}
			}
			cancel()
		}()
		_ = run(ctx)
		fmt.Println("--- simulated crash, restarting from checkpoint ---")
	}

	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}

	sink := engine.NewFileSink(resultsPath, latePath)
	results, err := sink.ReadResults()
	if err != nil {
		log.Fatal(err)
	}
	lates, err := sink.ReadLates()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("committed window results: %d, late records: %d\n", len(results), len(lates))
	var totalCount int64
	var totalSum float64
	for _, r := range results {
		totalCount += r.Count
		totalSum += r.Sum
	}
	fmt.Printf("global totals: count=%d sum=%.2f\n", totalCount, totalSum)
}

// genInput 生成乱序、含重复与迟到事件的 JSONL 输入。
func genInput(path string, n int) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	rng := rand.New(rand.NewSource(42))
	base := int64(1_700_000_000_000)
	keys := []string{"a", "b", "c", "d"}
	enc := json.NewEncoder(f)
	var prev engine.Event
	for i := 0; i < n; i++ {
		ev := engine.Event{
			ID:        fmt.Sprintf("ev-%d", i),
			Key:       keys[rng.Intn(len(keys))],
			EventTime: base + int64(i)*1000 + int64(rng.Intn(120_000)) - 60_000, // 乱序 ±60s
			Value:     float64(rng.Intn(100)) + 0.5,
		}
		if err := enc.Encode(ev); err != nil {
			return err
		}
		// 每 50 条原样重发上一条事件（模拟上游重试造成的重复）。
		if i > 0 && i%50 == 0 {
			if err := enc.Encode(prev); err != nil {
				return err
			}
		}
		prev = ev
	}
	return nil
}
