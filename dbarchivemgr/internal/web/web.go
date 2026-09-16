// Package web 提供管理后台静态界面（单页，纯 HTML/CSS/JS，无构建、无外部 CDN）。
// 经 go:embed 打进二进制，gin 挂 /ui；数据全部来自 /api/v1 只读列表接口。
package web

import (
	"embed"
	"io/fs"
	"net/http"

	"github.com/gin-gonic/gin"
)

//go:embed static
var staticFS embed.FS

// Register 注册管理后台路由：/ui 静态页 + 根路径重定向。
func Register(r *gin.Engine) {
	sub, err := fs.Sub(staticFS, "static")
	if err != nil {
		return
	}
	r.StaticFS("/ui", http.FS(sub))
	r.GET("/", func(c *gin.Context) { c.Redirect(http.StatusFound, "/ui/") })
}
