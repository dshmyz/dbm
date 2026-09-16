// Package auth 的 CAS 登录全链路测试：以 httptest 起假 CAS 服务端，
// 验证 401/302 防护、登录跳转、ticket 校验、会话 Cookie、白名单放行与注销。
package auth

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/dbarchivemgr/dbarchivemgr/internal/api"
	"github.com/dbarchivemgr/dbarchivemgr/internal/gfsclient"
	"github.com/dbarchivemgr/dbarchivemgr/internal/service"
	"github.com/dbarchivemgr/dbarchivemgr/internal/sim"
	"github.com/dbarchivemgr/dbarchivemgr/internal/store"
)

// newFakeCAS 假 CAS：/cas/login 颁发固定 ticket；/cas/p3/proxyValidate 校验
// （ST-good → 用户 zhangsan，其余拒绝）。
func newFakeCAS(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/cas/login":
			service := r.URL.Query().Get("service")
			http.Redirect(w, r, service+"&ticket=ST-good", http.StatusFound)
		case r.URL.Path == "/cas/p3/proxyValidate":
			ticket := r.URL.Query().Get("ticket")
			w.Header().Set("Content-Type", "application/xml")
			if ticket == "ST-good" {
				fmt.Fprintf(w, `<cas:serviceResponse xmlns:cas="http://www.yale.edu/tp/cas">
  <cas:authenticationSuccess>
    <cas:user>zhangsan</cas:user>
  </cas:authenticationSuccess>
</cas:serviceResponse>`)
				return
			}
			fmt.Fprintf(w, `<cas:serviceResponse xmlns:cas="http://www.yale.edu/tp/cas">
  <cas:authenticationFailure code="INVALID_TICKET">ticket %s 未被识别</cas:authenticationFailure>
</cas:serviceResponse>`, ticket)
		case r.URL.Path == "/cas/logout":
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// newAuthedApp 装配「假 CAS + 鉴权中间件 + 业务 API」的完整可测服务。
func newAuthedApp(t *testing.T, casURL string) (*httptest.Server, *httptest.Server) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	db, _ := store.New(":memory:")
	svc := service.New(db, &gfsclient.MemFileStore{GFS: sim.NewGFS("/gfs")})
	router := gin.New()
	mw := New(casURL, "/p3/proxyValidate", "")
	mw.Register(router)
	router.Use(mw.Require())
	(&api.Handler{Svc: svc, DB: db}).Register(router)
	ts := httptest.NewServer(router)
	t.Cleanup(ts.Close)
	return ts, nil
}

func TestDemoModePassthrough(t *testing.T) {
	ts, _ := newAuthedApp(t, "") // 未配 CAS → 演示模式
	// 一切放行
	res, err := http.Get(ts.URL + "/api/v1/clusters")
	if err != nil || res.StatusCode != 200 {
		t.Fatalf("演示模式应放行 API, got %d err=%v", res.StatusCode, err)
	}
	res.Body.Close()
	res, err = http.Get(ts.URL + "/auth/user")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var body struct {
		Data struct {
			User string `json:"user"`
		} `json:"data"`
	}
	json.NewDecoder(res.Body).Decode(&body)
	if body.Data.User != "demo" {
		t.Fatalf("演示模式 /auth/user 应返回 demo, got %q", body.Data.User)
	}
}

func TestCASLoginFlow(t *testing.T) {
	cas := newFakeCAS(t)
	ts, _ := newAuthedApp(t, cas.URL+"/cas")

	// client 不跟随重定向（中间 302 的 Set-Cookie 无 jar 不保留，避免登录回环）
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

	// 1) 未登录访问 API → 401 JSON
	res, err := http.Get(ts.URL + "/api/v1/clusters")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("未登录 API 应 401, got %d", res.StatusCode)
	}

	// 2) 未登录访问页面 → 302 跳 /auth/login
	res, err = client.Get(ts.URL + "/ui/")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusFound || !strings.HasPrefix(res.Header.Get("Location"), "/auth/login") {
		t.Fatalf("未登录页面应 302 → /auth/login, got %d loc=%q", res.StatusCode, res.Header.Get("Location"))
	}

	// 3) /auth/login → 302 跳 CAS 登录页（带 service 回调）
	res, err = client.Get(ts.URL + "/auth/login")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	loc := res.Header.Get("Location")
	if res.StatusCode != http.StatusFound || !strings.HasPrefix(loc, cas.URL+"/cas/login?service=") {
		t.Fatalf("/auth/login 应 302 → CAS 登录页, got %d loc=%q", res.StatusCode, loc)
	}

	// 4) 模拟 CAS 回调（带合法 ticket）→ 签发会话 Cookie → 302 回 /ui/
	res, err = client.Get(ts.URL + "/auth/callback?ticket=ST-good")
	if err != nil {
		t.Fatal(err)
	}
	var cookie *http.Cookie
	for _, ck := range res.Cookies() {
		if ck.Name == cookieName {
			cookie = ck
		}
	}
	res.Body.Close()
	if res.StatusCode != http.StatusFound || cookie == nil || cookie.Value == "" {
		t.Fatalf("回调应 302 且签发会话 Cookie, got %d cookie=%v", res.StatusCode, cookie)
	}

	// 5) 带 Cookie 访问受保护 API → 200
	req, _ := http.NewRequest("GET", ts.URL+"/api/v1/clusters", nil)
	req.AddCookie(cookie)
	res, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("带会话应放行 API, got %d", res.StatusCode)
	}

	// 6) /auth/user 返回 CAS 用户名
	req, _ = http.NewRequest("GET", ts.URL+"/auth/user", nil)
	req.AddCookie(cookie)
	res, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var ub struct {
		Data struct {
			User string `json:"user"`
		} `json:"data"`
	}
	json.NewDecoder(res.Body).Decode(&ub)
	res.Body.Close()
	if ub.Data.User != "zhangsan" {
		t.Fatalf("应返回 CAS 用户 zhangsan, got %q", ub.Data.User)
	}

	// 7) 机器回调白名单：无会话也不得 401（业务校验返回别的码）
	res, err = http.Post(ts.URL+"/api/v1/archive/result", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode == http.StatusUnauthorized {
		t.Fatal("白名单回调 /archive/result 不应要求登录")
	}
	res, err = http.Post(ts.URL+"/api/v1/replenish/task/status", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode == http.StatusUnauthorized {
		t.Fatal("白名单回调 /replenish/task/status 不应要求登录")
	}

	// 8) 注销后会话失效
	req, _ = http.NewRequest("POST", ts.URL+"/auth/logout", nil)
	req.AddCookie(cookie)
	res, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	req, _ = http.NewRequest("GET", ts.URL+"/api/v1/clusters", nil)
	req.AddCookie(cookie)
	res, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("注销后应回到 401, got %d", res.StatusCode)
	}
}

func TestCASInvalidTicket(t *testing.T) {
	cas := newFakeCAS(t)
	ts, _ := newAuthedApp(t, cas.URL+"/cas")
	res, err := http.Get(ts.URL + "/auth/callback?ticket=ST-bad")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("非法 ticket 应 401, got %d", res.StatusCode)
	}
}
