package config

import "os"

// RedisConfig 构建系统级别的核心网络访问声明结构体系，执行中间配置项注入
type RedisConfig struct {
	Host string
	Port string
	Addr string
}

// LoadRedisConfig 使用具备覆盖性和安全扩展性的环境变量抽象方法执行数据获取和预装配
func LoadRedisConfig() RedisConfig {
	host := os.Getenv("REDIS_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("REDIS_PORT")
	if port == "" {
		port = "6379"
	}
	return RedisConfig{
		Host: host,
		Port: port,
		Addr: host + ":" + port,
	}
}
