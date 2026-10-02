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
	// Code 是回给客户端的那个闭集错误码（cookie.go 里那十一个），"release" 表示这行是
	// 一次归还而不是失败。页面上必须看得见它：同一句人话底下可能是两种完全不同的
	// 处置（「等几小时」和「等几十秒」长得一样）。
	Code   string
	Reason string // 给上传者看的短句，和回给客户端的那句一致
	Detail string // 原始错误，只进管理页
}

// failLogKeep 留多少条。够看清「刚才那一阵」就行，不是账本。
const failLogKeep = 200

// failDetailMax 截断原始错误：它进的是 HTML 页面，不是日志。单位是**字符**。
const failDetailMax = 300

// failLog 是最近若干次交付失败的环形缓冲。
type failLog struct {
	mu   sync.Mutex
	rows []deliverFail
}

func (l *failLog) add(f deliverFail) {
	// 按字符截，不按字节：Detail 基本全是中文，按字节切会把半个汉字落进 HTML 页面
	// （同仓库的 sanitizeLabel 和 types.SampleErr 也都是按字符）。
	if r := []rune(f.Detail); len(r) > failDetailMax {
		f.Detail = string(r[:failDetailMax]) + "…"
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.rows = append(l.rows, f)
	if len(l.rows) > failLogKeep {
		// 整段搬一次而不是每条 append 都搬：这条路径上的量级是「偶尔一阵」。
		l.rows = append(l.rows[:0], l.rows[len(l.rows)-failLogKeep:]...)
	}
}

// notAFailure 报告这一行其实是一次**成功**：续期收下了新 cookie、归还了槽位。
//
// 它们和失败共用一个环（「续期在不在生效」和取票失败是同一个问题的两面），但上传者
// 那一页只留 20 条、标题写的是「最近的取票失败」、表头写的是「错误码」——而按冻结口径
// **每取一张票就续一次**，于是那 20 条会被一行行 `renew / 续到还剩 3023 秒` 占满：
// 既看不懂（这明明是成功），又把真正的失败挤出了窗口。管理页 200 条不过滤。
func (f deliverFail) notAFailure() bool { return f.Code == "renew" || f.Code == "release" }

// recent 回最近 n 条，新的在前。accountID 非 0 时只给那个账号的；
// failsOnly 时跳过续期/归还这类成功行（见 notAFailure）。
func (l *failLog) recent(accountID int64, n int, failsOnly bool) []deliverFail {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]deliverFail, 0, n)
	for i := len(l.rows) - 1; i >= 0 && len(out) < n; i-- {
		if accountID != 0 && l.rows[i].AccountID != accountID {
			continue
		}
		if failsOnly && l.rows[i].notAFailure() {
			continue
		}
		out = append(out, l.rows[i])
	}
	return out
}

// failRows 把它摊成页面上的行。
//
// admin 把「这是管理页吗」这一个判断管到底，两件事都跟着它走：
//   - Detail 只给管理页（里面是出口名、上游状态码这类内部读数）；
//   - 续期/归还那些成功行也只给管理页（见 notAFailure）。
//
// 两件事同一个分界，所以同一个参数，不另加一个布尔。
func (l *failLog) failRows(accountID int64, n int, admin bool) []failRow {
	fs := l.recent(accountID, n, !admin)
	out := make([]failRow, 0, len(fs))
	for _, f := range fs {
		r := failRow{At: f.At.UTC().Format("01-02 15:04:05"), AccountID: f.AccountID,
			Gateway: f.Gateway, Code: f.Code, Reason: f.Reason}
		if admin {
			r.Detail = f.Detail
		}
		out = append(out, r)
	}
	return out
}
