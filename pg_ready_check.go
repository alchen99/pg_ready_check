package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	// Exit codes
	ExitCodeOK            = 0
	ExitCodeConnFailed    = 1
	ExitCodeCheckFailed   = 2 // e.g., tables missing
	ExitCodeBadArgs       = 3
	ExitCodeInternalError = 4

	// Default values
	DefaultHost          = "localhost"
	DefaultPort          = 5432
	DefaultUser          = "postgres"       // Or get current OS user? pg_isready uses OS user
	DefaultDBName        = ""               // Depends on user, often same as user
	DefaultTimeout       = 60 * time.Second // Overall wait timeout
	DefaultConnTimeout   = 5 * time.Second  // Timeout for each connection attempt
	DefaultRetryInterval = 1 * time.Second  // Wait time between retries
)

func main() {
	// --- Configuration ---
	var (
		dbHost        string
		dbPort        int
		dbUser        string
		dbName        string
		dbPassword    string // Primarily via env var
		tablesToCheck string
		timeout       time.Duration
		connTimeout   time.Duration
		quiet         bool
		printVersion  bool
	)

	// Get OS user for default username if PGDATABASE is not set
	osUser, err := os.UserHomeDir() // Using home dir as a proxy for username often works, but might not be perfect
	if err == nil {
		parts := strings.Split(osUser, string(os.PathSeparator))
		osUser = parts[len(parts)-1]
	} else {
		osUser = "user" // Fallback
	}
	defaultUser := getEnvOrDefault("PGUSER", osUser)
	defaultDbName := getEnvOrDefault("PGDATABASE", defaultUser) // Often defaults to username

	flag.StringVar(&dbHost, "host", getEnvOrDefault("PGHOST", DefaultHost), "Database server host or socket directory (env: PGHOST)")
	flag.IntVar(&dbPort, "port", getEnvOrDefaultInt("PGPORT", DefaultPort), "Database server port (env: PGPORT)")
	flag.StringVar(&dbUser, "username", defaultUser, "Database user name (env: PGUSER)")
	flag.StringVar(&dbName, "dbname", defaultDbName, "Database name to connect to (env: PGDATABASE)")
	flag.StringVar(&tablesToCheck, "tables", "", "Comma-separated list of tables to check for existence (e.g., 'users,products')")
	flag.DurationVar(&timeout, "timeout", DefaultTimeout, "Maximum time to wait for connection and checks")
	flag.DurationVar(&connTimeout, "conn-timeout", DefaultConnTimeout, "Timeout for each connection attempt")
	flag.BoolVar(&quiet, "quiet", false, "Run quietly, only exit code matters")
	flag.BoolVar(&printVersion, "version", false, "Print version information and exit")

	// Custom usage message
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options]\n\n", os.Args[0])
		fmt.Fprintln(os.Stderr, "Options:")
		flag.PrintDefaults()
		fmt.Fprintln(os.Stderr, "\nEnvironment Variables:")
		fmt.Fprintln(os.Stderr, "  PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE can be used for connection parameters.")
		fmt.Fprintln(os.Stderr, "\nExit Status:")
		fmt.Fprintln(os.Stderr, "  0: Server is accepting connections (and tables exist if specified).")
		fmt.Fprintln(os.Stderr, "  1: Server connection failed (timeout, refused, etc.).")
		fmt.Fprintln(os.Stderr, "  2: Connection succeeded, but table check failed (tables missing).")
		fmt.Fprintln(os.Stderr, "  3: Invalid command-line arguments.")
		fmt.Fprintln(os.Stderr, "  4: Internal error.")
	}

	flag.Parse()

	if printVersion {
		// You might want to embed version info during build
		fmt.Println("pg_ready_check (Go version) 1.0.0")
		os.Exit(ExitCodeOK)
	}

	// Password from environment variable (best practice)
	dbPassword = os.Getenv("PGPASSWORD")

	if !quiet {
		log.Printf("Attempting to connect to database: host=%s port=%d user=%s dbname=%s",
			dbHost, dbPort, dbUser, dbName)
		if tablesToCheck != "" {
			log.Printf("Will also check for tables: [%s]", tablesToCheck)
		}
		log.Printf("Waiting up to %s for database to be ready...", timeout)
	}

	// --- Main Logic ---
	requiredTables := parseTableList(tablesToCheck)
	overallCtx, cancelOverall := context.WithTimeout(context.Background(), timeout)
	defer cancelOverall()

	startTime := time.Now()
	var lastErr error

	for {
		select {
		case <-overallCtx.Done():
			// Overall timeout exceeded
			logError(quiet, "Overall timeout (%s) exceeded. Last error: %v", timeout, lastErr)
			os.Exit(ExitCodeConnFailed) // Treat overall timeout as connection failure
		default:
			// Try connecting and checking
			attemptCtx, cancelAttempt := context.WithTimeout(overallCtx, connTimeout)
			conn, err := connectDB(attemptCtx, dbHost, dbPort, dbUser, dbPassword, dbName)
			cancelAttempt() // Release context resources promptly

			if err != nil {
				lastErr = fmt.Errorf("connection attempt failed: %w", err)
				logDebug(quiet, "%v", lastErr)
				time.Sleep(DefaultRetryInterval) // Wait before retrying
				continue                         // Try again
			}

			// --- Connection Successful ---
			logDebug(quiet, "Connection successful.")

			// --- Perform Table Check (if requested) ---
			if len(requiredTables) > 0 {
				tableCheckCtx, cancelTableCheck := context.WithTimeout(overallCtx, connTimeout) // Reuse connTimeout for query
				missingTables, err := checkTablesExist(tableCheckCtx, conn, requiredTables)
				cancelTableCheck()

				if err != nil {
					// Error during table check (not just missing tables)
					conn.Close(context.Background()) // Close connection on error
					lastErr = fmt.Errorf("error checking tables: %w", err)
					logError(quiet, "%v", lastErr)
					// Decide if this is retryable or fatal. Let's retry.
					time.Sleep(DefaultRetryInterval)
					continue
				}

				if len(missingTables) > 0 {
					conn.Close(context.Background()) // Close connection, tables not ready yet
					lastErr = fmt.Errorf("required tables missing: %s", strings.Join(missingTables, ", "))
					logDebug(quiet, "%v", lastErr)
					time.Sleep(DefaultRetryInterval) // Wait before retrying
					continue                         // Try again
				}
				logDebug(quiet, "All required tables [%s] found.", tablesToCheck)
			}

			// --- Success ---
			conn.Close(context.Background()) // Close the successful connection
			duration := time.Since(startTime).Round(time.Millisecond)
			logSuccess(quiet, "Database ready after %s.", duration)
			os.Exit(ExitCodeOK)
		}
	}
}

// --- Helper Functions ---

// getEnvOrDefault reads an environment variable or returns a default value.
func getEnvOrDefault(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

// getEnvOrDefaultInt reads an environment variable as an int or returns a default value.
func getEnvOrDefaultInt(key string, defaultValue int) int {
	if valueStr, exists := os.LookupEnv(key); exists {
		var value int
		_, err := fmt.Sscan(valueStr, &value)
		if err == nil {
			return value
		}
	}
	return defaultValue
}

// parseTableList splits the comma-separated string into a slice of table names.
func parseTableList(tables string) []string {
	if tables == "" {
		return nil
	}
	list := strings.Split(tables, ",")
	result := make([]string, 0, len(list))
	for _, t := range list {
		trimmed := strings.TrimSpace(t)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

// connectDB attempts to connect to the database and pings it.
func connectDB(ctx context.Context, host string, port int, user, password, dbname string) (*pgx.Conn, error) {
	// Construct DSN (Data Source Name)
	// Example: "postgres://user:password@host:port/dbname?sslmode=disable"
	dsn := fmt.Sprintf("postgres://%s@%s:%d/%s?sslmode=disable", user, host, port, dbname)
	// Add password if provided via PGPASSWORD
	if password != "" {
		// Note: Including password in DSN is generally less secure than libpq's mechanisms,
		// but common for tools like this. pgx handles PGPASSWORD if not in DSN.
		// Let's simplify and let pgx handle PGPASSWORD implicitly if not in DSN.
		dsn = fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", user, password, host, port, dbname)
	}

	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DSN: %w", err)
	}
	// pgx automatically uses PGPASSWORD if config.Password is empty and PGPASSWORD is set.

	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		// Mask password in error message if DSN was logged or part of error
		errMsg := strings.Replace(err.Error(), password, "[PASSWORD]", -1)
		return nil, errors.New(errMsg) // Return generic error type after masking
	}

	// Ping the database to verify the connection is live
	if err := conn.Ping(ctx); err != nil {
		conn.Close(context.Background()) // Close connection if ping fails
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return conn, nil
}

// checkTablesExist checks if all specified tables exist in the database.
// Returns a list of missing tables and an error if the query failed.
func checkTablesExist(ctx context.Context, conn *pgx.Conn, tables []string) ([]string, error) {
	missing := []string{}
	if len(tables) == 0 {
		return missing, nil // Nothing to check
	}

	// We check one by one for simplicity, could optimize with ANY($1) later if needed.
	// Assumes 'public' schema if not specified like 'schema.table'.
	query := `SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2 LIMIT 1`

	for _, table := range tables {
		schemaName := "public"
		tableName := table
		if strings.Contains(table, ".") {
			parts := strings.SplitN(table, ".", 2)
			schemaName = parts[0]
			tableName = parts[1]
		}

		var exists int
		err := conn.QueryRow(ctx, query, schemaName, tableName).Scan(&exists)

		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				// Table does not exist
				missing = append(missing, table)
				continue // Check next table
			}
			// An actual error occurred during the query
			return nil, fmt.Errorf("error querying for table '%s': %w", table, err)
		}
		// If Scan succeeds (err == nil), the table exists (exists == 1)
	}

	return missing, nil
}

// --- Logging Helpers ---

func logError(quiet bool, format string, args ...interface{}) {
	// Always log errors, even in quiet mode, but maybe to stderr?
	// pg_isready doesn't print errors in quiet mode. Let's follow that.
	if !quiet {
		log.Printf("ERROR: "+format, args...)
	}
}

func logSuccess(quiet bool, format string, args ...interface{}) {
	if !quiet {
		log.Printf(format, args...)
	}
}

func logDebug(quiet bool, format string, args ...interface{}) {
	// These are intermediate messages, only show when not quiet
	if !quiet {
		log.Printf(format, args...)
	}
}
