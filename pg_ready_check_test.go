package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestGetEnvOrDefault(t *testing.T) {
	os.Clearenv()

	testCases := []struct {
		name           string
		key            string
		defaultValue   string
		expectedValue  string
		envVarSet      bool
		envVarValue    string
		expectedOutput string
	}{
		{
			name:           "Environment variable set",
			key:            "TEST_VAR",
			defaultValue:   "default",
			envVarSet:      true,
			envVarValue:    "env_value",
			expectedValue:  "env_value",
			expectedOutput: "env_value",
		},
		{
			name:           "Environment variable not set",
			key:            "TEST_VAR",
			defaultValue:   "default",
			expectedValue:  "default",
			expectedOutput: "default",
		},
		{
			name:           "Empty default value",
			key:            "TEST_VAR",
			defaultValue:   "",
			expectedValue:  "",
			expectedOutput: "",
		},
		{
			name:           "Empty environment variable",
			key:            "TEST_VAR",
			defaultValue:   "default",
			envVarSet:      true,
			envVarValue:    "",
			expectedValue:  "",
			expectedOutput: "",
		},
		{
			name:           "Multiple chars",
			key:            "TEST_VAR",
			defaultValue:   "default1",
			envVarSet:      true,
			envVarValue:    "env_value1",
			expectedValue:  "env_value1",
			expectedOutput: "env_value1",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.envVarSet {
				os.Setenv(tc.key, tc.envVarValue)
			}
			result := getEnvOrDefault(tc.key, tc.defaultValue)

			if result != tc.expectedOutput {
				t.Errorf("Expected %s, got %s", tc.expectedOutput, result)
			}
		})
	}
}

func TestGetEnvOrDefaultInt(t *testing.T) {
	os.Clearenv()

	testCases := []struct {
		name          string
		key           string
		defaultValue  int
		envVarSet     bool
		envVarValue   string
		expectedValue int
		expectError   bool
	}{
		{
			name:          "Environment variable set and valid integer",
			key:           "TEST_INT",
			defaultValue:  10,
			envVarSet:     true,
			envVarValue:   "20",
			expectedValue: 20,
			expectError:   false,
		},
		{
			name:          "Environment variable not set",
			key:           "TEST_INT",
			defaultValue:  10,
			expectedValue: 10,
			expectError:   false,
		},
		{
			name:          "Environment variable set but invalid integer",
			key:           "TEST_INT",
			defaultValue:  10,
			envVarSet:     true,
			envVarValue:   "invalid",
			expectedValue: 0,
			expectError:   false,
		},
		{
			name:          "Zero default value",
			key:           "TEST_INT",
			expectedValue: 0,
			expectError:   false,
		},
		{
			name:          "Negative default value",
			key:           "TEST_INT",
			defaultValue:  -5,
			expectedValue: -5,
			expectError:   false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.envVarSet {
				os.Setenv(tc.key, tc.envVarValue)
			}
			result := getEnvOrDefaultInt(tc.key, tc.defaultValue)
			if result != tc.expectedValue {
				if tc.expectError {
					t.Fatalf("Expected error but got: %d", result)
				}
				t.Fatalf("Expected %d, got %d", tc.expectedValue, result)
			}
		})
		os.Clearenv()
	}
}

func TestParseTableList(t *testing.T) {
	testCases := []struct {
		name           string
		input          string
		expectedOutput []string
		expectError    bool
	}{
		{
			name:           "Valid table list",
			input:          "table1,table2,table3",
			expectedOutput: []string{"table1", "table2", "table3"},
			expectError:    false,
		},
		{
			name:           "Empty input",
			input:          "",
			expectedOutput: []string{},
			expectError:    false,
		},
		{
			name:           "Single table",
			input:          "table1",
			expectedOutput: []string{"table1"},
			expectError:    false,
		},
		{
			name:           "Extra commas",
			input:          ",table1,,table2,",
			expectedOutput: []string{"", "table1", "", "table2", ""},
			expectError:    false,
		},
		{
			name:           "White space",
			input:          " table1 , table2 , table3 ",
			expectedOutput: []string{" table1 ", " table2 ", " table3 "},
			expectError:    false,
		},
		{
			name:           "Mix of valid and empty",
			input:          ",table1,,table2,table3,",
			expectedOutput: []string{"", "table1", "", "table2", "table3", ""},
			expectError:    false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := parseTableList(tc.input)
			if !reflect.DeepEqual(result, tc.expectedOutput) {
				t.Errorf("Expected %v, got %v", tc.expectedOutput, result)
			}
		})
	}
}

func TestConnectDB(t *testing.T) {
	os.Clearenv()

	testCases := []struct {
		name        string
		envVars     map[string]string
		expectError bool
	}{
		{
			name: "Valid connection",
			envVars: map[string]string{
				"POSTGRES_HOST":     "localhost",
				"POSTGRES_PORT":     "5432",
				"POSTGRES_USER":     "postgres",
				"POSTGRES_PASSWORD": "password",
				"POSTGRES_DB":       "postgres",
			},
			expectError: false,
		},
		{
			name: "Missing host",
			envVars: map[string]string{
				"POSTGRES_PORT":     "5432",
				"POSTGRES_USER":     "postgres",
				"POSTGRES_PASSWORD": "password",
				"POSTGRES_DB":       "postgres",
			},
			expectError: true,
		},
		{
			name: "Missing port",
			envVars: map[string]string{
				"POSTGRES_HOST":     "localhost",
				"POSTGRES_USER":     "postgres",
				"POSTGRES_PASSWORD": "password",
				"POSTGRES_DB":       "postgres",
			},
			expectError: true,
		},
		{
			name: "Missing user",
			envVars: map[string]string{
				"POSTGRES_HOST":     "localhost",
				"POSTGRES_PORT":     "5432",
				"POSTGRES_PASSWORD": "password",
				"POSTGRES_DB":       "postgres",
			},
			expectError: true,
		},
		{
			name: "Missing password",
			envVars: map[string]string{
				"POSTGRES_HOST": "localhost",
				"POSTGRES_PORT": "5432",
				"POSTGRES_USER": "postgres",
				"POSTGRES_DB":   "postgres",
			},
			expectError: true,
		},
		{
			name: "Missing db",
			envVars: map[string]string{
				"POSTGRES_HOST":     "localhost",
				"POSTGRES_PORT":     "5432",
				"POSTGRES_USER":     "postgres",
				"POSTGRES_PASSWORD": "password",
			},
			expectError: true,
		},
		{
			name:        "No env vars",
			envVars:     map[string]string{},
			expectError: true,
		},
		{
			name: "Empty env vars",
			envVars: map[string]string{
				"POSTGRES_HOST":     "",
				"POSTGRES_PORT":     "",
				"POSTGRES_USER":     "",
				"POSTGRES_PASSWORD": "",
				"POSTGRES_DB":       "",
			},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for key, value := range tc.envVars {
				_ = os.Setenv(key, value)
			}
			_, err := connectDB(context.Background(), getEnvOrDefault("POSTGRES_HOST", DefaultHost), getEnvOrDefaultInt("POSTGRES_PORT", DefaultPort), getEnvOrDefault("POSTGRES_USER", "postgres"), getEnvOrDefault("POSTGRES_PASSWORD", "password"), getEnvOrDefault("POSTGRES_DB", "postgres"))
			if tc.expectError {
				if err == nil {
					t.Errorf("Expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}
			}
			os.Clearenv()
		})
	}
}

func TestCheckTablesExist(t *testing.T) {
	os.Clearenv()
	dsn := fmt.Sprintf("host=localhost port=5432 user=postgres password=password dbname=postgres sslmode=disable")
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("Error connecting to test DB: %v", err)
	}
	db, err := pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("Error connecting to db table: %v", err)
	}
	defer db.Close(context.Background())
	createTables := []string{
		"CREATE TABLE IF NOT EXISTS test_table (id SERIAL PRIMARY KEY)",
		"CREATE TABLE IF NOT EXISTS test_table2 (id SERIAL PRIMARY KEY)",
		"CREATE TABLE IF NOT EXISTS test_table3 (id SERIAL PRIMARY KEY)",
	}
	for _, query := range createTables {
		_, err = db.Exec(context.Background(), query)
		if err != nil {
			t.Fatalf("Error creating db table: %v", err)
		}
	}

	dropTables := []string{
		"DROP TABLE IF EXISTS test_table",
		"DROP TABLE IF EXISTS test_table2",
		"DROP TABLE IF EXISTS test_table3",
	}
	defer func() {
		db.Close(context.Background())

		for _, query := range dropTables {
			_, err = db.Exec(context.Background(), query)
			if err != nil {
				t.Fatalf("Error dropping db table: %v", err)
			}
		}
	}()

	testCases := []struct {
		name            string
		tableList       []string
		schema          string
		expectError     bool
		expectedMissing int
		expectedError   string
	}{
		{
			name:            "All tables exist",
			tableList:       []string{"test_table", "test_table2"},
			expectError:     false,
			expectedError:   "",
			expectedMissing: 0,
			schema:          "public",
		},
		{
			name:          "Some tables exist",
			tableList:     []string{"test_table", "nonexistent_table"},
			expectError:   true,
			expectedError: "table nonexistent_table does not exist",
		},
		{
			name:          "No tables exist",
			tableList:     []string{"nonexistent_table1", "nonexistent_table2"},
			expectError:   true,
			expectedError: "table nonexistent_table1 does not exist",
		},
		{
			name:          "Empty table list",
			tableList:     []string{},
			expectError:   false,
			expectedError: "",
		},
		{
			name:          "Single table exists",
			tableList:     []string{"test_table"},
			expectError:   false,
			expectedError: "",
		},
		{
			name:          "Single table does not exist",
			tableList:     []string{"nonexistent_table"},
			expectError:   true,
			expectedError: "table nonexistent_table does not exist",
		},
		{
			name:          "Duplicate table exists",
			tableList:     []string{"test_table", "test_table"},
			expectError:   false,
			expectedError: "",
		},
		{
			name:          "Duplicate table does not exist",
			tableList:     []string{"nonexistent_table", "nonexistent_table"},
			expectError:   true,
			expectedError: "table nonexistent_table does not exist",
		},
		{
			name:          "Table does exist at end",
			tableList:     []string{"nonexistent_table", "test_table"},
			expectError:   true,
			expectedError: "table nonexistent_table does not exist",
		},
		{
			name:          "Multiple tables exist",
			tableList:     []string{"test_table", "test_table2", "test_table3"},
			expectError:   false,
			expectedError: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			missing, err := checkTablesExist(context.Background(), db, tc.tableList)
			if tc.expectError {
				if err == nil {
					t.Errorf("Expected error, but got nil")
				} else if err.Error() != tc.expectedError {
					t.Errorf("Expected error message '%s', got '%s'", tc.expectedError, err.Error())
				}
			} else {
				if len(missing) != tc.expectedMissing && !errors.Is(err, pgx.ErrNoRows) {
					t.Fatalf("Expected missing table count %d, but got %d and err: %v", tc.expectedMissing, len(missing), err)
				}
				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}
			}
		})
	}
}
