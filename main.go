package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	_ "github.com/lib/pq" // для PostgreSQL
)

// Конфигурация приложения
type Config struct {
	ConnStrings  string        // Строки подключения к БД (через запятую)
	SQLFile      string        // Имя файла с SQL-запросом
	SQLQuery     string        // или SQL-запрос
	Timeout      time.Duration // Таймаут выполнения запроса
	Limit        int           // Ограничение количества возвращаемых строк
	Parallel     bool          // Параллельное выполнение запросов
	OutputFormat string        // Формат вывода результата
	DebugEnable  bool          // Включить Debug логи
}

const (
	DEFAULT_LIMIT           = 200
	DEFAULT_OUTPUT_FORMAT   = TableFormat
	DEFAULT_TIMEOUT         = time.Second * 5
	DEFAULT_HEADER_COL_NAME = "DB_№"
)

type OutputFormat = string

const (
	TableFormat OutputFormat = "table"
	CsvFormat   OutputFormat = "csv"
)

func parseOutputFormat(s string) (OutputFormat, error) {
	s = strings.ToLower(s)
	switch s {
	case TableFormat, CsvFormat:
		return s, nil
	default:
		return TableFormat, fmt.Errorf("неподдерживаемый формат вывода")
	}
}

func getOutputFormatVariants() []OutputFormat {
	return []OutputFormat{TableFormat, CsvFormat}
}

func loadConfig() (*Config, error) {
	cfg := &Config{}

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Использование qgun:\n\n")
		fmt.Fprintf(flag.CommandLine.Output(), "qgun [FLAGS] -q SQL_QUERY [ARGS]:\n")
		fmt.Fprintf(flag.CommandLine.Output(), "qgun [FLAGS] -f SQL_FILE [ARGS]:\n")
		fmt.Fprintf(flag.CommandLine.Output(), "\n")
		flag.PrintDefaults()
	}

	// Флаги командной строки
	flag.StringVar(&cfg.ConnStrings, "c", "", "DB_CONN_STRINGS: (Строки подключения к БД (через запятую)")
	flag.StringVar(&cfg.SQLFile, "f", "", "SQL_FILE: Имя файла с SQL-запросом")
	flag.StringVar(&cfg.SQLQuery, "q", "", "SQL_QUERY: SQL-запрос")
	flag.DurationVar(&cfg.Timeout, "t", DEFAULT_TIMEOUT, "QUERY_TIMEOUT: Таймаут выполнения запроса")
	flag.IntVar(&cfg.Limit, "l", DEFAULT_LIMIT, "MAX_ROWS: Ограничение количества возвращаемых строк (0 - без ограничения)")
	flag.StringVar(
		&cfg.OutputFormat,
		"o",
		DEFAULT_OUTPUT_FORMAT,
		fmt.Sprintf("OUTPUT_FORMAT: Формат вывода возвращаемого результата %v", getOutputFormatVariants()),
	)
	flag.BoolVar(&cfg.Parallel, "p", false, "PARALLEL_MODE: Параллельное выполнение запросов")
	flag.BoolVar(&cfg.DebugEnable, "v", false, "Влючение debug")

	// Парсим флаги
	flag.Parse()

	// Если параметры не установлены флагами, пробуем получить из переменных окружения
	if cfg.ConnStrings == "" {
		cfg.ConnStrings = os.Getenv("DB_CONN_STRINGS")
	}
	if cfg.SQLFile == "" {
		cfg.SQLFile = os.Getenv("SQL_FILE")
	}
	if cfg.SQLQuery == "" {
		cfg.SQLQuery = os.Getenv("SQL_QUERY")
	}
	if cfg.Timeout == DEFAULT_TIMEOUT { // Значение по умолчанию
		if envVal := os.Getenv("QUERY_TIMEOUT"); envVal != "" {
			if timeout, err := time.ParseDuration(envVal); err == nil {
				cfg.Timeout = timeout
			}
		}
	}
	if cfg.Limit == DEFAULT_LIMIT { // Значение по умолчанию
		if envVal := os.Getenv("MAX_ROWS"); envVal != "" {
			if limit, err := strconv.Atoi(envVal); err == nil {
				cfg.Limit = limit
			}
		}
	}
	if !cfg.Parallel {
		if envVal := os.Getenv("PARALLEL_MODE"); envVal != "" {
			if parallel, err := strconv.ParseBool(envVal); err == nil {
				cfg.Parallel = parallel
			}
		}
	}

	if cfg.OutputFormat == DEFAULT_OUTPUT_FORMAT {
		if envVal := os.Getenv("OUTPUT_FORMAT"); envVal != "" {
			if format, err := parseOutputFormat(envVal); err == nil {
				cfg.OutputFormat = format
			} else {
				return cfg, err
			}
		}
	}

	// Проверка обязательных параметров
	if cfg.ConnStrings == "" {
		return cfg, fmt.Errorf("не указана строка подключения")
	}
	if cfg.SQLFile == "" && cfg.SQLQuery == "" {
		return cfg, fmt.Errorf("требуется либо файл запроса, либо сам запрос")
	}
	if cfg.Limit < 0 {
		return cfg, fmt.Errorf("лимит не может быть отрицательным")
	}

	return cfg, nil
}

func main() {
	// Загрузка конфигурации
	cfg, err := loadConfig()
	if err != nil {
		slog.Error("ошибка конфигурации", "err", err)
		flag.Usage()
		os.Exit(1)
	}

	if cfg.DebugEnable {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}
	slog.Debug("", "cfg", cfg)

	query := getQuery(cfg)
	query_args := getQueryArgs()
	slog.Debug("", "query_args", query_args)

	rows_ch, header_ch, cleanup := setupOutputWrites(cfg, os.Stdout)

	ctx := context.Background()

	// Разбиваем строки подключения
	connections := strings.Split(cfg.ConnStrings, ",")
	has_errors := false

	if cfg.Parallel {
		var wg sync.WaitGroup
		for i, connection := range connections {
			wg.Add(1)
			connName := fmt.Sprintf("DB_%d", i)
			go func() {
				defer wg.Done()
				err := ProcessConnection(ctx, connName, connection, cfg.Limit, cfg.Timeout, header_ch, rows_ch, query, query_args...)
				if err != nil {
					slog.Error("Не удалось обработать запрос к БД", "connName", connName, "err", err)
					has_errors = true
				}
			}()
		}
		slog.Debug("Ожидание обработки всех коннектов")
		wg.Wait()
		cleanup()
	} else {
		for i, connection := range connections {
			connName := fmt.Sprintf("DB_%d", i)
			err := ProcessConnection(ctx, connName, connection, cfg.Limit, cfg.Timeout, header_ch, rows_ch, query, query_args...)
			if err != nil {
				slog.Error("Не удалось обработать запрос к БД", "connName", connName, "err", err)
				has_errors = true
			}
		}
		cleanup()
	}

	if has_errors {
		os.Exit(1)
	}

}

func setupOutputWrites(cfg *Config, output *os.File) (chan<- []string, chan<- []string, func()) {
	rows_ch := make(chan []string, DEFAULT_LIMIT)
	header_ch := make(chan []string, 1)

	var wg sync.WaitGroup
	wg.Add(1)

	processChannels := func(writeFunc func([]string) error) {
		slog.Debug("running channel process")
		header_row := <-header_ch
		slog.Debug("write header")
		err := writeFunc(header_row)
		if err != nil {
			slog.Error("can't write header", "err", err)
		}
		slog.Debug("stop processing headers channel")
		slog.Debug("start processing rows channel")
		for row := range rows_ch {
			slog.Debug("write row", "db", row[0])
			err := writeFunc(row)
			if err != nil {
				slog.Error("can't write row", "err", err)
			}
		}
		slog.Debug("stop channel process")
	}

	switch cfg.OutputFormat {
	case TableFormat:
		slog.Debug("running TabWriter export in backgroud")
		tw := tabwriter.NewWriter(output, 0, 0, 1, ' ', 0)
		writeRow := func(row []string) error {
			for _, val := range row {
				_, err := fmt.Fprintf(tw, "%v\t", val)
				if err != nil {
					return err
				}
			}
			_, err := fmt.Fprintln(tw)
			if err != nil {
				return err
			}
			return nil
		}

		go func() {
			defer wg.Done()
			defer tw.Flush()
			processChannels(writeRow)
		}()

	case CsvFormat:
		slog.Debug("running CsvWriter export in backgroud")
		cw := csv.NewWriter(output)

		go func() {
			defer wg.Done()
			defer cw.Flush()
			processChannels(cw.Write)
		}()
	default:
		panic("not implemented case of cfg.OutputFormat")
	}

	cleanup := func() {
		slog.Debug("close headers chan")
		close(header_ch)
		slog.Debug("close rows chan")
		close(rows_ch)
		slog.Debug("wait writer")
		wg.Wait()
		slog.Debug("writes done")
	}

	return rows_ch, header_ch, cleanup
}

func getQueryArgs() []any {
	args := flag.Args()
	queryArgs := make([]any, len(args))
	for i, arg := range args {
		queryArgs[i] = arg
	}
	return queryArgs
}

func getQuery(cfg *Config) string {
	var query string

	if cfg.SQLQuery != "" {
		query = cfg.SQLQuery
	}
	if cfg.SQLFile != "" {
		if cfg.SQLQuery == "" {
			query_b, err := os.ReadFile(cfg.SQLFile)
			if err != nil {
				slog.Error("Ошибка чтения файла", "err", err)
				os.Exit(1)
			}
			query = string(query_b)
		} else {
			slog.Warn("Будет использован запрос заданный напрямую", "query", query)
		}
	}
	return query
}

func ProcessConnection(ctx context.Context, connName string, connection string, limit int, timeout time.Duration, header_channel chan<- []string, rows_channel chan<- []string, query string, query_args ...any) error {
	db, err := sql.Open("postgres", connection)
	if err != nil {
		return fmt.Errorf("ошибка создания подключения к БД: %w", err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		return fmt.Errorf("ошибка подключения к БД: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	qc, err := NewLimitedQueryContext(ctx, db, limit, query, query_args...)
	if err != nil {
		return fmt.Errorf("ошибка выполнения запроса к бд QUERY(%v) ARGS(%v): %w", query, query_args, err)
	}
	defer qc.Close()

	header := append([]string{DEFAULT_HEADER_COL_NAME}, qc.Columns()...)

	select {
	case header_channel <- header:
		slog.Debug("header sended", "db", connName)
	default:
		slog.Debug("skip sending header", "db", connName)
	}

	slog.Debug("process rows", "connName", connName)
	for qc.Next() {
		row, err := qc.Scan()
		if err != nil {
			return fmt.Errorf("ошибка сканирования строки: %w", err)
		}

		row = append([]string{connName}, row...)

		rows_channel <- row

	}
	if err := qc.Err(); err != nil {
		return fmt.Errorf("ошибка при итерации: %w", err)
	}
	slog.DebugContext(ctx, "process rows comlite", "connName", connName)
	return nil
}

type LimitedQueryContext struct {
	rowCounter     int
	rowLimit       int
	rows           *sql.Rows
	col_names      []string
	buf_values     []any
	buf_valuesPtrs []any
}

func (q *LimitedQueryContext) Close() (_ error) {
	return q.rows.Close()
}

func NewLimitedQueryContext(ctx context.Context, db *sql.DB, limit int, query string, args ...any) (*LimitedQueryContext, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	values := make([]any, len(colTypes))
	valuesRtrs := make([]any, len(colTypes))
	col_names := make([]string, len(colTypes))

	for i, ct := range colTypes {
		valuesRtrs[i] = &values[i]
		col_names[i] = ct.Name()
	}

	result := LimitedQueryContext{
		rowCounter:     0,
		rowLimit:       limit,
		rows:           rows,
		col_names:      col_names,
		buf_values:     values,
		buf_valuesPtrs: valuesRtrs,
	}

	return &result, nil
}

func (q *LimitedQueryContext) Next() bool {
	return q.rows.Next()
}

func formatSqlValue(val any) string {
	var strVal string
	switch v := val.(type) {
	case nil:
		strVal = "NULL"
	case []byte:
		strVal = string(v)
	default:
		strVal = fmt.Sprintf("%v", v)
	}
	return strVal
}

func (q *LimitedQueryContext) Scan() ([]string, error) {
	if q.rowLimit > 0 && q.rowCounter >= q.rowLimit {
		return nil, NewLimitReachedErr(q.rowLimit)
	}
	if err := q.rows.Scan(q.buf_valuesPtrs...); err != nil {
		return nil, err
	}

	row := make([]string, len(q.buf_values)+1)

	for i, v := range q.buf_values {
		row[i] = formatSqlValue(v)
	}

	q.rowCounter += 1

	return row, nil
}

func (q *LimitedQueryContext) Err() error {
	return q.rows.Err()
}

func (q *LimitedQueryContext) Columns() []string {
	return q.col_names
}

type LimitReachedErr struct {
	limit int
}

func NewLimitReachedErr(limit int) *LimitReachedErr {
	return &LimitReachedErr{limit}
}

func (e *LimitReachedErr) Error() string {
	return fmt.Sprintf("достигнут лимит (%d) количества строк на запрос", e.limit)
}
