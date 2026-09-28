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

// 本地演示：生成包含乱序、重复与迟到事件的输入文件，
// 运行引擎进行事件时间窗口聚合，并打印结果与指标。
func main() {
	dir := flag.String("dir", "demo-data", "工作目录（输入/输出/检查点）")
	nEvents := flag.Int("events", 2000, "生成事件数量")
	crash := flag.Bool("crash", false, "模拟中途崩溃并重启恢复")
	retain := flag.Int("retain-checkpoints", 3, "保留的检查点代数（超出按 ID 删除）")
	recoverGen := flag.Uint64("recover-from", 0, "从指定代检查点恢复（0 表示最新代）")
	maxPending := flag.Int("max-pending-output", 10000, "尚未成功写出的记录上限（窗口结果+迟到记录）")
	latePolicy := flag.String("late-overflow", "reject", "迟到记录达到待写出上限时的策略：reject|block")
	sinkFaults := flag.Int("sink-faults", 0, "让前 N 次写出在写出 1 条记录后失败（模拟磁盘写一半，验证进程内续写）")
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
	var policy engine.LateOverflowPolicy
	switch *latePolicy {
	case "reject":
		policy = engine.LateOverflowReject
	case "block":
		policy = engine.LateOverflowBlock
	default:
		log.Fatalf("unknown -late-overflow=%q (want reject|block)", *latePolicy)
	}
	cfg := engine.Config{
		WindowSizeMs:          60_000, // 1 分钟窗口
		AllowedLatenessMs:     30_000, // 允许迟到 30 秒
		NumShards:             4,
		Workers:               4,
		QueueCapacity:         32,
		CheckpointEvery:       200,
		Logger:                logger,
		RetainCheckpoints:     *retain,
		RecoverFromGeneration: *recoverGen,
		MaxPendingOutput:      *maxPending,
		LateOverflow:          policy,
	}

	run := func(ctx context.Context, faults int) error {
		src := engine.NewFileSource(inputPath)
		defer src.Close()
		sink := engine.NewFileSink(resultsPath, latePath)
		if faults > 0 {
			// 前 faults 次写出各在落盘 1 条后失败：模拟磁盘写一半，引擎自动续写补齐。
			sink.FailNextCommits(faults)
		}
		store := engine.NewCheckpointStore(cpPath)
		e := engine.NewEngine(cfg, src, sink, store)
		if err := e.Run(ctx); err != nil {
			return err
		}
		st := e.Stats()
		fmt.Printf("stats: processed=%d duplicates=%d late=%d late_rejected=%d backpressure_blocks=%d "+
			"checkpoints=%d checkpoint_prunes=%d fork_prunes=%d flush_triggers=%d window_defers=%d sink_retries=%d sink_replays=%d\n",
			st.Processed, st.Duplicates, st.Late, st.LateRejected, st.BackpressureBlocks,
			st.Checkpoints, st.CheckpointPrunes, st.ForkPrunes, st.FlushTriggers, st.WindowDefers,
			st.SinkRetries, st.SinkReplays)
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
		_ = run(ctx, *sinkFaults)
		fmt.Println("--- simulated crash, restarting from checkpoint ---")
	} else if *sinkFaults > 0 {
		// 不崩溃、只注入写出故障：验证进程内自动续写，结果与无故障一致。
		if err := run(context.Background(), *sinkFaults); err != nil {
			log.Fatal(err)
		}
		return
	}

	if err := run(context.Background(), 0); err != nil {
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
