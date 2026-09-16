// Package auth 提供管理后台/REST 接口的 CAS 单点登录对接（CAS 2/3 标准协议：
// 浏览器跳转 CAS 登录页 → 携 ticket 回调 → 后端调 CAS /p3/proxyValidate 校验 → 签发本地会话 Cookie）。
//
// 未配置 -cas-url 时为【演示模式】：全部放行、不鉴权，保证 demo.sh 与本地开发不受影响；
// 配置后：UI 与 /api/v1/* 均要求登录，仅机器回调接口（大数据组件回调）与 /healthz 放行。
package auth

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

const (
	cookieName = "dbam_session"
	sessionTTL = 12 * time.Hour
)

// session 本地会话（服务端存储；Cookie 只存随机 token）。
type session struct {
	User      string
	ExpiresAt time.Time
}

// Middleware CAS 鉴权中间件 + 会话管理。
type Middleware struct {
	CASURL       string // CAS 服务根地址（如 https://cas.example.com/cas）；空 = 演示模式不鉴权
	ValidatePath string // ticket 校验路径（默认 /p3/proxyValidate；CAS 2.x 服务端可用 /serviceValidate）
	PublicURL    string // 本服务对外地址（构造 service 参数用；空则按请求 Host 推导）

	mu       sync.Mutex
	sessions map[string]session
}

// New 构造 CAS 中间件。casURL 为空即演示模式（放行一切）。
func New(casURL, validatePath, publicURL string) *Middleware {
	if validatePath == "" {
		validatePath = "/p3/proxyValidate"
	}
	return &Middleware{
		CASURL:       strings.TrimRight(casURL, "/"),
		ValidatePath: validatePath,
		PublicURL:    publicURL,
		sessions:     map[string]session{},
	}
}

// Register 挂载 /auth/* 端点（须在 Require 中间件之前注册）。
func (m *Middleware) Register(r gin.IRouter) {
	g := r.Group("/auth")
	g.GET("/login", m.handleLogin)
	g.GET("/callback", m.handleCallback)
	g.Any("/logout", m.handleLogout)
	g.GET("/user", m.handleUser)
}

// whitelist 机器回调放行清单（组件间调用无浏览器会话）：大数据组件回调接口 + 探活。
var whitelist = map[string]bool{
	"/api/v1/archive/result":        true,
	"/api/v1/replenish/task/status": true,
	"/healthz":                      true,
}

// Require 全局鉴权中间件：注册在 /auth 路由之后、业务路由之前。
func (m *Middleware) Require() gin.HandlerFunc {
	return func(c *gin.Context) {
		if m.CASURL == "" { // 演示模式：不鉴权
			c.Next()
			return
		}
		p := c.Request.URL.Path
		if whitelist[p] {
			c.Next()
			return
		}
		if s, ok := m.lookup(cookieValue(c)); ok {
			c.Set("user", s.User)
			c.Next()
			return
		}
		// 未登录：REST API 返回 401 JSON；页面请求 302 跳 CAS 登录
		if strings.HasPrefix(p, "/api/") {
			c.AbortWithStatusJSON(http.StatusUnauthorized,
				gin.H{"code": 401, "message": "未登录或会话已过期", "data": nil})
			return
		}
		c.Redirect(http.StatusFound, "/auth/login")
		c.Abort()
	}
}

/* ---------------- /auth 端点 ---------------- */

// handleLogin 跳转 CAS 登录页（演示模式直接签发 demo 会话）。
func (m *Middleware) handleLogin(c *gin.Context) {
	if m.CASURL == "" {
		m.setSession(c, "demo")
		c.Redirect(http.StatusFound, "/ui/")
		return
	}
	loginURL := fmt.Sprintf("%s/login?service=%s", m.CASURL, url.QueryEscape(m.serviceOf(c.Request)))
	c.Redirect(http.StatusFound, loginURL)
}

// handleCallback CAS 回调：校验 ticket → 签发会话 → 回管理后台。
func (m *Middleware) handleCallback(c *gin.Context) {
	ticket := c.Query("ticket")
	user, err := m.validateTicket(ticket, m.serviceOf(c.Request))
	if err != nil {
		c.String(http.StatusUnauthorized, "CAS 登录失败：%v", err)
		return
	}
	m.setSession(c, user)
	c.Redirect(http.StatusFound, "/ui/")
}

// handleLogout 注销本地会话；配了 CAS 时联动跳 CAS 登出。
func (m *Middleware) handleLogout(c *gin.Context) {
	if tok := cookieValue(c); tok != "" {
		m.mu.Lock()
		delete(m.sessions, tok)
		m.mu.Unlock()
	}
	c.SetCookie(cookieName, "", -1, "/", "", m.isSecure(c.Request), true)
	if m.CASURL != "" {
		c.Redirect(http.StatusFound, m.CASURL+"/logout?service="+url.QueryEscape(m.serviceOf(c.Request)))
		return
	}
	c.Redirect(http.StatusFound, "/ui/")
}

// handleUser 当前登录用户（前端顶栏展示用）。
func (m *Middleware) handleUser(c *gin.Context) {
	if s, ok := m.lookup(cookieValue(c)); ok {
		c.JSON(http.StatusOK, gin.H{"code": 0, "message": "success", "data": gin.H{"user": s.User, "authed": true}})
		return
	}
	if m.CASURL == "" { // 演示模式视为已登录
		c.JSON(http.StatusOK, gin.H{"code": 0, "message": "success", "data": gin.H{"user": "demo", "authed": true, "demo": true}})
		return
	}
	c.JSON(http.StatusUnauthorized, gin.H{"code": 401, "message": "未登录", "data": nil})
}

/* ---------------- CAS ticket 校验（CAS 2/3 XML 协议） ---------------- */

// casResponse CAS /p3/proxyValidate（兼容 /serviceValidate）响应。
// XMLName 不指定命名空间 → 按本地名匹配，兼容各版本 xmlns。
type casResponse struct {
	Success *struct {
		User       string `xml:"user"`
		Attributes *struct {
			Mail     []string `xml:"mail"`
			UserName []string `xml:"userName"`
		} `xml:"attributes"`
	} `xml:"authenticationSuccess"`
	Failure *struct {
		Code    string `xml:"code,attr"`
		Message string `xml:",chardata"`
	} `xml:"authenticationFailure"`
}

var casClient = &http.Client{Timeout: 10 * time.Second}

// validateTicket 调 CAS 校验 ticket，成功返回用户名。
func (m *Middleware) validateTicket(ticket, service string) (string, error) {
	if ticket == "" {
		return "", fmt.Errorf("缺少 ticket 参数")
	}
	u := fmt.Sprintf("%s%s?ticket=%s&service=%s",
		m.CASURL, m.ValidatePath, url.QueryEscape(ticket), url.QueryEscape(service))
	resp, err := casClient.Get(u)
	if err != nil {
		return "", fmt.Errorf("请求 CAS 校验接口失败: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("CAS 校验接口返回 %d", resp.StatusCode)
	}
	var cr casResponse
	if err := xml.NewDecoder(resp.Body).Decode(&cr); err != nil {
		return "", fmt.Errorf("解析 CAS 响应失败: %w", err)
	}
	switch {
	case cr.Success != nil && cr.Success.User != "":
		return cr.Success.User, nil
	case cr.Failure != nil:
		return "", fmt.Errorf("ticket 校验被拒绝(%s): %s", cr.Failure.Code, strings.TrimSpace(cr.Failure.Message))
	default:
		return "", fmt.Errorf("CAS 响应中无认证结果")
	}
}

/* ---------------- 会话存储 ---------------- */

func (m *Middleware) setSession(c *gin.Context, user string) {
	buf := make([]byte, 24)
	if _, err := rand.Read(buf); err != nil {
		return
	}
	tok := hex.EncodeToString(buf)
	m.mu.Lock()
	m.sessions[tok] = session{User: user, ExpiresAt: time.Now().Add(sessionTTL)}
	m.mu.Unlock()
	c.SetCookie(cookieName, tok, int(sessionTTL.Seconds()), "/", "", m.isSecure(c.Request), true)
}

func (m *Middleware) lookup(tok string) (session, bool) {
	if tok == "" {
		return session{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[tok]
	if !ok {
		return session{}, false
	}
	if time.Now().After(s.ExpiresAt) {
		delete(m.sessions, tok)
		return session{}, false
	}
	return s, true
}

func cookieValue(c *gin.Context) string {
	v, _ := c.Cookie(cookieName)
	return v
}

// serviceOf 计算 service 回调地址：优先 -public-url，否则按请求 Host/X-Forwarded-Proto 推导。
func (m *Middleware) serviceOf(r *http.Request) string {
	base := m.PublicURL
	if base == "" {
		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}
		if p := r.Header.Get("X-Forwarded-Proto"); p != "" {
			scheme = p
		}
		base = scheme + "://" + r.Host
	}
	return strings.TrimRight(base, "/") + "/auth/callback"
}

func (m *Middleware) isSecure(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https") || r.TLS != nil
}
