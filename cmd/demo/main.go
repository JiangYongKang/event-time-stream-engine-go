// demo 是一个可在本地直接运行的端到端示例：
//
//	go run ./cmd/demo
//
// 它构造两路分区的乱序事件（含重复、迟到），在处理中途用 context 取消模拟
// 崩溃，随后以全新进程语义的引擎从检查点恢复继续处理，最终打印对账结果。
// 工作目录下生成：
//
//	demo-out/checkpoint/    检查点清单与数据
//	demo-out/results.jsonl  窗口结果（幂等追加）
//	demo-out/late.jsonl     迟到旁路记录
//	demo-out/events-*.jsonl 输入数据源（两阶段相同，恢复依赖读取位点）
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/highcumontoa/event-time-stream-engine-go/pkg/checkpoint"
	"github.com/highcumontoa/event-time-stream-engine-go/pkg/engine"
	"github.com/highcumontoa/event-time-stream-engine-go/pkg/model"
	"github.com/highcumontoa/event-time-stream-engine-go/pkg/sink"
	"github.com/highcumontoa/event-time-stream-engine-go/pkg/source"
)

type fileEvent struct {
	ID        string    `json:"id"`
	Key       string    `json:"key"`
	EventTime time.Time `json:"event_time"`
	Value     int64     `json:"value"`
	Seq       int64     `json:"seq,omitempty"`
}

func mustWriteEvents(path string, evs []model.Event) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		log.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	for _, ev := range evs {
		if err := enc.Encode(fileEvent{
			ID: ev.ID, Key: ev.Key, EventTime: ev.EventTime, Value: ev.Value, Seq: ev.Seq,
		}); err != nil {
			log.Fatal(err)
		}
	}
}

// buildEvents 返回单分区的完整事件流。
// 阶段1处理前 2 个批次（4 条，时间戳都很小）后崩溃，窗口远未定稿；
// 恢复后前 4 条从位点重放（同 ID 重复不重复计入），随后水位推进：
// 乱序但在允许迟到内的事件被接受，超出范围的事件被明确旁路。
func buildEvents(base time.Time) map[int][]model.Event {
	ev := func(id, key string, second int64, val int64, p int, seq int64) model.Event {
		return model.Event{ID: id, Key: key, EventTime: base.Add(time.Duration(second) * time.Second),
			Value: val, Partition: p, Seq: seq}
	}
	p0 := []model.Event{
		// 阶段1 崩溃前（batch=2，崩溃发生在第 2 批已应用但位点未推进、未提交时；
		// 两批时间戳都很小，崩溃时水位停留在 3-5=负数 ~ 0，窗口远未定稿）
		ev("e-00", "alpha", 2, 1, 0, 1),
		ev("e-01", "alpha", 3, 4, 0, 2),
		ev("e-02", "alpha", 8, 2, 0, 3),
		ev("e-03", "alpha", 9, 8, 0, 4),
		// ---- 以下为恢复后处理（前 4 条会从位点重放）----
		ev("e-04", "alpha", 50, 20, 0, 6), // 推进事件时间到 50（水位≈45）
		ev("e-00", "alpha", 2, 1, 0, 7),   // 同 ID 重复（重放后再次出现）-> 不重复计入
		ev("e-05", "alpha", 6, 8, 0, 8),   // 乱序（下一批）：6+30>=45，在允许迟到内 -> 接受
		ev("e-06", "alpha", 61, 10, 0, 9),
		ev("e-07", "alpha", 95, 16, 0, 10), // wm 越过 90 后窗口0（end60+30）定稿
		ev("e-08", "alpha", 5, 50, 0, 11),  // 真迟到：5+30<水位 -> 明确旁路
		ev("e-09", "alpha", 200, 100, 0, 12),
	}
	return map[int][]model.Event{0: p0}
}

func main() {
	outDir := flag.String("out", "demo-out", "output directory")
	crash := flag.Bool("crash", true, "simulate crash and restart from checkpoint")
	flag.Parse()

	log.SetFlags(log.Ltime)
	base := time.Date(2026, 9, 22, 10, 0, 0, 0, time.UTC)
	cpDir := filepath.Join(*outDir, "checkpoint")
	resPath := filepath.Join(*outDir, "results.jsonl")
	latePath := filepath.Join(*outDir, "late.jsonl")
	p0Path := filepath.Join(*outDir, "events-0.jsonl") // 单分区演示；多分区并发见单元测试

	events := buildEvents(base)
	mustWriteEvents(p0Path, events[0])
	os.RemoveAll(cpDir)
	os.Remove(resPath)
	os.Remove(latePath)

	cfg := engine.Config{
		WindowSize:      time.Minute,
		OutOfOrderness:  5 * time.Second,
		AllowedLateness: 60 * time.Second,
		BatchSize:       2,
		PendingQueueCap: 2,                      // 故意调小以暴露背压
		CheckpointEvery: 200 * time.Millisecond, // 崩溃前不触发检查点（恢复后重放）
		ProcessDelay:    2 * time.Millisecond,
	}

	run := func(tag string, crashAfterBatches int64) {
		src, err := source.NewFileSource(map[int]string{0: p0Path})
		if err != nil {
			log.Fatal(err)
		}
		snk, err := sink.NewFileSink(resPath, latePath)
		if err != nil {
			log.Fatal(err)
		}
		runCfg := cfg
		runCfg.CrashAfterBatches = crashAfterBatches
		eng := engine.New(runCfg, src, snk, checkpoint.NewManager(cpDir))

		err = eng.Run(context.Background())
		if crashAfterBatches > 0 && err != nil {
			fmt.Printf("---- %s: 模拟崩溃（%v）：内存中已处理但未提交，重启后按位点重放\n", tag, err)
		} else if err != nil {
			log.Fatalf("%s run: %v", tag, err)
		}

		m := eng.SnapshotMetrics()
		restored := "<全新启动>"
		if m.RestoredFrom != "" {
			restored = m.RestoredFrom
		}
		fmt.Printf("---- %s 指标: 处理=%d 重复丢弃=%d 迟到旁路=%d 提交窗口=%d 检查点=%d 背压=%d 恢复自=%s\n",
			tag, m.Processed, m.DuplicateDropped, m.LateSideOutput, m.WindowsCommitted,
			m.Checkpoints, m.BackPressure, restored)
		if err := snk.Close(); err != nil {
			log.Fatal(err)
		}
	}

	if *crash {
		fmt.Println("========== 阶段 1：确定性崩溃（处理 4 条后，位点未推进、结果未提交） ==========")
		run("阶段1", 2)

		fmt.Println("========== 阶段 2：新引擎从检查点恢复，处理到源末尾 ==========")
		run("阶段2-恢复", 0)
	} else {
		run("单次运行", 0)
	}

	fmt.Println("========== 最终对账 ==========")
	v, _ := sink.NewFileSink(resPath, latePath)
	defer v.Close()
	fmt.Printf("已提交窗口结果（共 %d 个）:\n", len(v.Results()))
	for _, r := range v.Results() {
		fmt.Printf("  key=%-5s [%s,%s) count=%d sum=%d id=%s\n",
			r.Key, r.Start.Format("15:04:05"), r.End.Format("15:04:05"),
			r.Count, r.Sum, r.ID)
	}
	fmt.Printf("迟到旁路记录（共 %d 条，均未静默丢弃）:\n", len(v.Late()))
	for _, l := range v.Late() {
		fmt.Printf("  event=%s key=%s eventTime=%s reason=%s watermark=%s\n",
			l.Event.ID, l.Event.Key, l.Event.EventTime.Format("15:04:05"),
			l.Reason, l.Watermark.Format("15:04:05"))
	}
	fmt.Println("\n输出目录:", *outDir)
}
