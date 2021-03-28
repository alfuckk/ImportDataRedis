module github.com/mishupaf-create/ImportDataRedis

go 1.16

replace github.com/mishupaf-create/ImportDataRedis/db => ./db

require (
	github.com/go-redis/redis/v8 v8.8.0 // indirect
	github.com/satori/go.uuid v1.2.0 // indirect
)
