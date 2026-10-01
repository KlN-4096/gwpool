package httpapi

import (
	"sync"
	"time"
)

// deliverFail 是一次没发出票的交付。
//
// 为什么要留：客户端那头只看到一个 502/503，上传者对着它猜不出是「池子不对外开放」
// 还是「你在所有网关上都烧完了」。服务端日志有，但要 SSH 才看得到。
//
// 进程内存、有上限、不落盘：它回答的是「刚才那一发怎么了」，重启清零正合适。
// 落盘就要多一个文件格式和一轮 fsync，而长期指标是另一件事。
type deliverFail struct {
	At        time.Time
	AccountID int64 // 0 = 匿名调用
	Gateway   string
	Reason    string // 给上传者看的短句，和回给客户端的那句一致
	Detail    string // 原始错误，只进管理页
}

// failLogKeep 留多少条。够看清「刚才那一阵」就行，不是账本。
const failLogKeep = 200

// failDetailMax 截断原始错误：它进的是 HTML 页面，不是日志。
const failDetailMax = 300

// failLog 是最近若干次交付失败的环形缓冲。
type failLog struct {
	mu   sync.Mutex
	rows []deliverFail
}

func (l *failLog) add(f deliverFail) {
	if n := len(f.Detail); n > failDetailMax {
		f.Detail = f.Detail[:failDetailMax] + "…"
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.rows = append(l.rows, f)
	if len(l.rows) > failLogKeep {
		// 整段搬一次而不是每条 append 都搬：这条路径上的量级是「偶尔一阵」。
		l.rows = append(l.rows[:0], l.rows[len(l.rows)-failLogKeep:]...)
	}
}

// recent 回最近 n 条，新的在前。accountID 非 0 时只给那个账号的。
func (l *failLog) recent(accountID int64, n int) []deliverFail {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]deliverFail, 0, n)
	for i := len(l.rows) - 1; i >= 0 && len(out) < n; i-- {
		if accountID != 0 && l.rows[i].AccountID != accountID {
			continue
		}
		out = append(out, l.rows[i])
	}
	return out
}

// failRows 把它摊成页面上的行。Detail 只给管理页：里面是出口名、上游状态码这类内部读数。
func (l *failLog) failRows(accountID int64, n int, withDetail bool) []failRow {
	fs := l.recent(accountID, n)
	out := make([]failRow, 0, len(fs))
	for _, f := range fs {
		r := failRow{At: f.At.UTC().Format("01-02 15:04:05"), AccountID: f.AccountID,
			Gateway: f.Gateway, Reason: f.Reason}
		if withDetail {
			r.Detail = f.Detail
		}
		out = append(out, r)
	}
	return out
}
