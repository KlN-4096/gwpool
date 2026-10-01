// Package web 把页面模板嵌进二进制：单文件部署，不依赖运行目录。
package web

import "embed"

//go:embed templates
var Templates embed.FS
