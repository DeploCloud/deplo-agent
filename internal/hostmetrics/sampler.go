package hostmetrics

import "time"

const minWindow = 50 * time.Millisecond

// Sampler holds the previous CPU/net counters so successive Sample() calls derive rates from the REAL elapsed time between them instead of sleeping.
type Sampler struct {
	dataDir string

	prevCPU   cpuTimes
	prevCPUOK bool
	prevRx    int64
	prevTx    int64
	prevNetOK bool
	prevAt    time.Time

	readCPU func() (cpuTimes, bool)
	readNet func() (rx, tx int64, ok bool)
}

// NewSampler primes the baseline by reading the counters NOW, so the first Sample() one tick later already spans a real window.
func NewSampler(dataDir string) *Sampler {
	if dataDir == "" {
		dataDir = "/"
	}
	s := &Sampler{
		dataDir: dataDir,
		readCPU: readCPUTimesOK,
		readNet: readNetCountersOK,
		prevAt:  time.Now(),
	}
	s.prevCPU, s.prevCPUOK = s.readCPU()
	s.prevRx, s.prevTx, s.prevNetOK = s.readNet()
	return s
}

// Sample takes a snapshot WITHOUT blocking.
func (s *Sampler) Sample() Metrics {
	now := time.Now()
	cpu, cpuOK := s.readCPU()
	rx, tx, netOK := s.readNet()

	mem := readMem()
	memUsed := mem.total - mem.available
	if memUsed < 0 {
		memUsed = 0
	}
	memTotal := mem.total
	diskUsed, diskTotal, diskAvail := diskBytes(s.dataDir)
	l1, l5, l15 := loadavg()

	m := Metrics{
		CPUCores:  numCPU(),
		MemUsed:   memUsed,
		MemTotal:  memTotal,
		MemFree:   mem.free,
		MemCache:  mem.cache,
		DiskUsed:  diskUsed,
		DiskTotal: diskTotal,
		Load1:     l1,
		Load5:     l5,
		Load15:    l15,
		UptimeSec: uptimeSec(),
	}
	if memTotal > 0 {
		m.MemPct = round1(float64(memUsed) / float64(memTotal) * 100)
	}
	m.DiskPct = diskPercent(diskUsed, diskAvail)

	window := now.Sub(s.prevAt)
	if window < minWindow {
		return m
	}
	elapsed := window.Seconds()

	if cpuOK && s.prevCPUOK {
		m.CPU = cpuPercent(s.prevCPU, cpu)
	}
	if netOK && s.prevNetOK {
		m.NetRx = perSecond(max(0, rx-s.prevRx), elapsed)
		m.NetTx = perSecond(max(0, tx-s.prevTx), elapsed)
	}

	if cpuOK {
		s.prevCPU, s.prevCPUOK = cpu, true
	}
	if netOK {
		s.prevRx, s.prevTx, s.prevNetOK = rx, tx, true
		s.prevAt = now
	}
	return m
}

func perSecond(delta int64, elapsed float64) int64 {
	return int64(float64(delta)/elapsed + 0.5)
}
