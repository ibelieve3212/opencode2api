package pool

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// 健康检查必须反映"进程活着"，而不是"上游有没有额度"。
//
// 2026-11-26 那次上游整体 429 期间，容器一直标 unhealthy：健康池本来就是空的，
// 如果把 /healthz 也绑到"有没有可用出口"，编排器就会不停重启一个其实没毛病的
// 进程，把攒下的 probeN/quotaN 台账和健康池全丢掉，越重启越糟。
func TestHealthzStaysOKWithoutHealthyExits(t *testing.T) {
	p, err := New(DefaultConfig(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	l := &Listener{Pool: p}

	rec := httptest.NewRecorder()
	l.HealthMux().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/healthz 有总池无健康出口时应 200，实际 %d", rec.Code)
	}

	// 一条出口都没有时，仍然要 200：进程活着这件事跟池子里有没有货无关。
	rec = httptest.NewRecorder()
	l.HealthMux().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/healthz 空池时应 200，实际 %d", rec.Code)
	}
}

// /readyz 是另一回事：一条出口都派不出去就该说不行，让上游编排器看到。
func TestReadyzReportsNothingToDispatch(t *testing.T) {
	p, err := New(DefaultConfig(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	l := &Listener{Pool: p}

	rec := httptest.NewRecorder()
	l.HealthMux().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz 空池时应 503，实际 %d", rec.Code)
	}

	// 总池有货就说明门槛在过、只是还没出健康出口，这是正常中途状态。
	if added, _ := p.UpsertMaster(&Exit{IP: "203.0.113.7", Port: 6300, Kind: KindExt}); !added {
		t.Fatal("入池失败")
	}
	rec = httptest.NewRecorder()
	l.HealthMux().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/readyz 总池有货时应 200，实际 %d", rec.Code)
	}
}
