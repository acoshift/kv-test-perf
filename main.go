package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

func main() {
	kv, err := NewSQLKV("postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable")
	// kv, err := NewRedisKV("localhost:6379")
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	err = kv.Setup(ctx)
	if err != nil {
		panic(err)
	}

	const n = 100
	const d = 10 * time.Second

	fmt.Printf("backend: %s\n", kv.Name())

	{
		fmt.Printf("==== set ====\n")
		ctx, _ := context.WithTimeout(ctx, d)

		start := time.Now()
		s := Stats{}
		for i := 0; i < n; i++ {
			i := i
			go runSet(ctx, kv, i, &s)
		}

		<-ctx.Done()
		t := time.Since(start)

		fmt.Printf("total: %d\n", s.ok+s.err)
		fmt.Printf("ops: %d\n", int64(s.ok+s.err)/int64(t/time.Second))
		fmt.Printf("ok: %d\n", s.ok)
		fmt.Printf("err: %d\n", s.err)
	}

	{
		fmt.Printf("==== get ====\n")
		ctx, _ := context.WithTimeout(ctx, d)

		start := time.Now()
		s := Stats{}
		for i := 0; i < n; i++ {
			i := i
			go runGet(ctx, kv, i, &s)
		}

		<-ctx.Done()
		t := time.Since(start)

		fmt.Printf("total: %d\n", s.ok+s.err)
		fmt.Printf("ops: %d\n", int64(s.ok+s.err)/int64(t/time.Second))
		fmt.Printf("ok: %d\n", s.ok)
		fmt.Printf("err: %d\n", s.err)
	}
}

type Stats struct {
	ok  uint64
	err uint64
}

func (s *Stats) OK() {
	atomic.AddUint64(&s.ok, 1)
}

func (s *Stats) Err(err error) {
	if errors.Is(err, context.DeadlineExceeded) {
		return
	}
	fmt.Println(err)
	atomic.AddUint64(&s.err, 1)
}

func runSet(ctx context.Context, kv KV, i int, s *Stats) {
	key := fmt.Sprintf("key_%d", i)
	value := fmt.Sprintf("value_%d", i)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		err := kv.Set(ctx, key, value)
		if err != nil {
			s.Err(err)
			continue
		}

		s.OK()
	}
}

func runGet(ctx context.Context, kv KV, i int, s *Stats) {
	key := fmt.Sprintf("key_%d", i)
	value := fmt.Sprintf("value_%d", i)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		v, err := kv.Get(ctx, key)
		if err != nil {
			s.Err(err)
			continue
		}

		if v != value {
			s.Err(fmt.Errorf("unexpected value: %s", v))
			continue
		}

		s.OK()
	}
}

type KV interface {
	Name() string
	Setup(ctx context.Context) error
	Set(ctx context.Context, key, value string) error
	Get(ctx context.Context, key string) (string, error)
}

type sqlKV struct {
	db *sql.DB
}

func NewSQLKV(uri string) (KV, error) {
	db, err := sql.Open("postgres", uri)
	if err != nil {
		return nil, err
	}
	db.SetMaxIdleConns(30)
	return &sqlKV{db: db}, nil
}

func (s *sqlKV) Name() string {
	return "postgresql"
}

func (s *sqlKV) Setup(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
		drop table if exists kv;
		create unlogged table kv(k varchar primary key, v varchar)
	`)
	return err
}

func (s *sqlKV) Set(ctx context.Context, key, value string) error {
	_, err := s.db.ExecContext(ctx, `insert into kv(k, v) values($1, $2) on conflict (k) do update set v = excluded.v`, key, value)
	return err
}

func (s *sqlKV) Get(ctx context.Context, key string) (string, error) {
	var value string
	err := s.db.QueryRowContext(ctx, `select v from kv where k = $1`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	return value, err
}

type redisKV struct {
	client *redis.Client
}

func NewRedisKV(addr string) (KV, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		MaxIdleConns: 30,
	})
	return &redisKV{client: client}, nil
}

func (r *redisKV) Name() string {
	return "redis"
}

func (r *redisKV) Setup(ctx context.Context) error {
	return nil
}

func (r *redisKV) Set(ctx context.Context, key, value string) error {
	return r.client.Set(ctx, key, value, 0).Err()
}

func (r *redisKV) Get(ctx context.Context, key string) (string, error) {
	return r.client.Get(ctx, key).Result()
}
