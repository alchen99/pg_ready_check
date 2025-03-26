package main

import (
	"context"
	"testing"
	"strconv"
	"strings"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupTestContainer(t *testing.T) (testcontainers.Container, string) {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "postgres:14",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "test",
			"POSTGRES_PASSWORD": "test",
			"POSTGRES_DB":       "testdb",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)

	port, err := container.MappedPort(ctx, "5432")
	require.NoError(t, err)

	host, err := container.Host(ctx)
	require.NoError(t, err)

	return container, host + ":" + port.Port()
}

func splitHostPort(addr string) (string, int) {
	parts := strings.Split(addr, ":")
	if len(parts) != 2 {
		return addr, 0
	}

	port, err := strconv.Atoi(parts[1])
	if err != nil {
		return addr, 0
	}

	host := parts[0]

	return host, port
}

func TestDatabaseIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	container, addr := setupTestContainer(t)
    defer container.Terminate(context.Background())

    ctx := context.Background()
    
    host, port := splitHostPort(addr)
    
	conn, err := connectDB(ctx, host, port, "test", "test", "testdb")

	require.NoError(t, err)
	defer conn.Close(ctx)

	// Create test tables
	_, err = conn.Exec(ctx, `
        CREATE TABLE users (id SERIAL PRIMARY KEY);
        CREATE TABLE products (id SERIAL PRIMARY KEY);
    `)
	require.NoError(t, err)

	// Test table existence checks
	tables := []string{"users", "products"}
	missing, err := checkTablesExist(ctx, conn, tables)
	assert.NoError(t, err)
	assert.Empty(t, missing)

	// Test non-existent table
	missing, err = checkTablesExist(ctx, conn, []string{"nonexistent"})
	assert.NoError(t, err)
	assert.Equal(t, []string{"nonexistent"}, missing)
}
