package logger

import (
	"log/slog"
	"os"
)

// InitLogger 实现面向全体系生产环境监控框架要求的系统日志构建抽象实现（依据 JSON 格式规范）
func InitLogger(serviceName string) {
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo, // 高等级要求配置化下沉为 Info 以提供全链路支持追查
	}
	
	// 在原有的数据记录基础之上扩展全局的静态日志服务集群身份标识（指纹服务标识增强检索能效）
	handler := slog.NewJSONHandler(os.Stdout, opts).WithAttrs([]slog.Attr{
		slog.String("service", serviceName),
	})
	
	logger := slog.New(handler)
	slog.SetDefault(logger)
}
