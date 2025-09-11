package service_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mibrgmv/payment-service/services/account/internal/repository/postgres"
	"github.com/mibrgmv/payment-service/services/account/internal/service"
	"github.com/mibrgmv/payment-service/services/account/internal/service/models"
	postgresshared "github.com/mibrgmv/payment-service/shared/postgres"
)

func TestBalanceProcessor_ProcessBalanceChangeEvent_Success(t *testing.T) {
	// Setup test database connection
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	// Create repositories
	balanceRepo := postgres.NewBalanceRepository(db)
	eventTrackingRepo := postgres.NewEventTrackingRepository(db)
	processor := service.NewBalanceProcessor(balanceRepo, eventTrackingRepo, db)

	ctx := context.Background()

	// Test data
	accountID := uuid.New().String()
	userID := uuid.New().String()
	initialBalance := int64(10000)                        // $100.00 in cents
	changeAmount := int64(-2500)                          // -$25.00 debit
	expectedFinalBalance := initialBalance + changeAmount // $75.00

	// Setup: Create test account and balance
	setupTestAccount(t, db, accountID, userID, initialBalance)

	// Create test event
	event := models.BalanceChangeEvent{
		EventID:   "test_event_" + uuid.New().String(),
		AccountID: accountID,
		Amount:    changeAmount,
		Timestamp: time.Now(),
		Source:    "transaction_service",
	}

	// Serialize event to JSON (simulating Kafka message)
	eventBytes, err := json.Marshal(event)
	require.NoError(t, err)

	// Act: Process the event
	err = processor.ProcessBalanceChangeEvent(ctx, eventBytes)

	// Assert: No error occurred
	assert.NoError(t, err)

	// Verify: Balance was updated correctly
	balance, err := balanceRepo.GetBalance(ctx, accountID)
	require.NoError(t, err)
	assert.Equal(t, float64(expectedFinalBalance), balance.Amount)
	assert.Equal(t, accountID, balance.AccountID)

	// Verify: Event was marked as processed
	processed, err := eventTrackingRepo.IsEventProcessed(ctx, event.EventID)
	require.NoError(t, err)
	assert.True(t, processed)

	// Verify: Event exists in processed_events table
	var eventExists bool
	err = db.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM processed_events WHERE event_id = $1 AND account_id = $2)",
		event.EventID, accountID).Scan(&eventExists)
	require.NoError(t, err)
	assert.True(t, eventExists)
}

func TestBalanceProcessor_ProcessBalanceChangeEvent_Idempotency(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	balanceRepo := postgres.NewBalanceRepository(db)
	eventTrackingRepo := postgres.NewEventTrackingRepository(db)
	processor := service.NewBalanceProcessor(balanceRepo, eventTrackingRepo, db)

	ctx := context.Background()

	// Test data
	accountID := uuid.New().String()
	userID := uuid.New().String()
	initialBalance := int64(10000)
	changeAmount := int64(1000) // +$10.00 credit

	setupTestAccount(t, db, accountID, userID, initialBalance)

	event := models.BalanceChangeEvent{
		EventID:   "idempotent_test_" + uuid.New().String(),
		AccountID: accountID,
		Amount:    changeAmount,
		Timestamp: time.Now(),
		Source:    "transaction_service",
	}

	eventBytes, err := json.Marshal(event)
	require.NoError(t, err)

	// Act: Process the event first time
	err = processor.ProcessBalanceChangeEvent(ctx, eventBytes)
	assert.NoError(t, err)

	// Get balance after first processing
	balanceAfterFirst, err := balanceRepo.GetBalance(ctx, accountID)
	require.NoError(t, err)
	expectedBalance := initialBalance + changeAmount
	assert.Equal(t, float64(expectedBalance), balanceAfterFirst.Amount)

	// Act: Process the same event again
	err = processor.ProcessBalanceChangeEvent(ctx, eventBytes)
	assert.NoError(t, err)

	// Verify: Balance should be the same
	balanceAfterSecond, err := balanceRepo.GetBalance(ctx, accountID)
	require.NoError(t, err)
	assert.Equal(t, float64(expectedBalance), balanceAfterSecond.Amount)
	assert.Equal(t, balanceAfterFirst.Amount, balanceAfterSecond.Amount)

	// Verify: Event still marked as processed only once
	var eventCount int
	err = db.QueryRow(ctx,
		"select count(*) from processed_events where event_id = $1",
		event.EventID).Scan(&eventCount)
	require.NoError(t, err)
	assert.Equal(t, 1, eventCount)
}

func TestBalanceProcessor_ProcessBalanceChangeEvent_AccountNotFound(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	balanceRepo := postgres.NewBalanceRepository(db)
	eventTrackingRepo := postgres.NewEventTrackingRepository(db)
	processor := service.NewBalanceProcessor(balanceRepo, eventTrackingRepo, db)

	ctx := context.Background()

	// Test with non-existent account
	nonExistentAccountID := uuid.New().String()
	event := models.BalanceChangeEvent{
		EventID:   "nonexistent_account_" + uuid.New().String(),
		AccountID: nonExistentAccountID,
		Amount:    1000,
		Timestamp: time.Now(),
		Source:    "transaction_service",
	}

	eventBytes, err := json.Marshal(event)
	require.NoError(t, err)

	// Act: Process event for non-existent account
	err = processor.ProcessBalanceChangeEvent(ctx, eventBytes)

	// Should not return error
	assert.NoError(t, err)

	// Verify: Event should still be marked as processed
	processed, err := eventTrackingRepo.IsEventProcessed(ctx, event.EventID)
	require.NoError(t, err)
	assert.True(t, processed)
}

func TestBalanceProcessor_ProcessBalanceChangeEvent_InvalidJSON(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	balanceRepo := postgres.NewBalanceRepository(db)
	eventTrackingRepo := postgres.NewEventTrackingRepository(db)
	processor := service.NewBalanceProcessor(balanceRepo, eventTrackingRepo, db)

	ctx := context.Background()

	// Act: Process invalid JSON
	invalidJSON := []byte(`{"invalid": "json" missing closing brace`)
	err := processor.ProcessBalanceChangeEvent(ctx, invalidJSON)

	// Assert: Should return error
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal balance change event")
}

func TestBalanceProcessor_ProcessBalanceChangeEvent_MultipleEvents(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	balanceRepo := postgres.NewBalanceRepository(db)
	eventTrackingRepo := postgres.NewEventTrackingRepository(db)
	processor := service.NewBalanceProcessor(balanceRepo, eventTrackingRepo, db)

	ctx := context.Background()

	// Test data
	accountID := uuid.New().String()
	userID := uuid.New().String()
	initialBalance := int64(10000) // $100.00

	setupTestAccount(t, db, accountID, userID, initialBalance)

	// Process multiple events
	events := []models.BalanceChangeEvent{
		{
			EventID:   "event_1_" + uuid.New().String(),
			AccountID: accountID,
			Amount:    500, // +$5.00
			Timestamp: time.Now(),
			Source:    "transaction_service",
		},
		{
			EventID:   "event_2_" + uuid.New().String(),
			AccountID: accountID,
			Amount:    -200, // -$2.00
			Timestamp: time.Now(),
			Source:    "transaction_service",
		},
		{
			EventID:   "event_3_" + uuid.New().String(),
			AccountID: accountID,
			Amount:    1000, // +$10.00
			Timestamp: time.Now(),
			Source:    "transaction_service",
		},
	}

	expectedFinalBalance := initialBalance + 500 - 200 + 1000 // $113.00

	// Process all events
	for _, event := range events {
		eventBytes, err := json.Marshal(event)
		require.NoError(t, err)

		err = processor.ProcessBalanceChangeEvent(ctx, eventBytes)
		assert.NoError(t, err)
	}

	// Verify final balance
	balance, err := balanceRepo.GetBalance(ctx, accountID)
	require.NoError(t, err)
	assert.Equal(t, float64(expectedFinalBalance), balance.Amount)

	// Verify all events were processed
	for _, event := range events {
		processed, err := eventTrackingRepo.IsEventProcessed(ctx, event.EventID)
		require.NoError(t, err)
		assert.True(t, processed)
	}
}

func setupTestDB(t *testing.T) *pgxpool.Pool {
	dbURL := "postgres://bill_clinton:2001@localhost:5434/account_service_test?sslmode=disable"
	db, err := pgxpool.New(context.Background(), dbURL)
	require.NoError(t, err)
	migrationPath := "../migrations"
	err = postgresshared.MigrateUp(db, migrationPath)
	require.NoError(t, err)
	return db
}

func cleanupTestDB(t *testing.T, db *pgxpool.Pool) {
	ctx := context.Background()

	_, err := db.Exec(ctx, "TRUNCATE TABLE processed_events CASCADE")
	require.NoError(t, err)
	_, err = db.Exec(ctx, "TRUNCATE TABLE balances CASCADE")
	require.NoError(t, err)
	_, err = db.Exec(ctx, "TRUNCATE TABLE accounts CASCADE")
	require.NoError(t, err)

	//db.Close()
}

func setupTestAccount(t *testing.T, db *pgxpool.Pool, accountID, userID string, initialBalance int64) {
	ctx := context.Background()

	// Insert test account
	_, err := db.Exec(ctx, `
        insert into accounts (account_id, user_id, currency, created_at, updated_at)
        values ($1, $2, 'USD', now(), now())
    `, accountID, userID)
	require.NoError(t, err)

	// Insert initial balance
	_, err = db.Exec(ctx, `
        insert into balances (account_id, amount, last_updated)
        values ($1, $2, now())
    `, accountID, initialBalance)
	require.NoError(t, err)
}

//// Benchmark test to ensure performance
//func BenchmarkBalanceProcessor_ProcessBalanceChangeEvent(b *testing.B) {
//	db := setupTestDB(b)
//	defer cleanupTestDB(b, db)
//
//	balanceRepo := postgres.NewBalanceRepository(db)
//	eventTrackingRepo := postgres.NewEventTrackingRepository(db)
//	processor := service.NewBalanceProcessor(balanceRepo, eventTrackingRepo, db)
//
//	ctx := context.Background()
//	accountID := uuid.New().String()
//	userID := uuid.New().String()
//	setupTestAccount(b, db, accountID, userID, 100000)
//
//	b.ResetTimer()
//	for i := 0; i < b.N; i++ {
//		event := models.BalanceChangeEvent{
//			EventID:   "bench_event_" + uuid.New().String(),
//			AccountID: accountID,
//			Amount:    int64(i % 1000), // Vary amounts
//			Timestamp: time.Now(),
//			Source:    "transaction_service",
//		}
//
//		eventBytes, _ := json.Marshal(event)
//		processor.ProcessBalanceChangeEvent(ctx, eventBytes)
//	}
//}
//
//// Helper functions for benchmark that match testing.B interface
//func setupTestDB(t interface{
//	Helper()
//	Errorf(format string, args ...interface{})
//	FailNow()
//}) *pgxpool.Pool {
//	t.Helper()
//
//	dbURL := "postgres://bill_clinton:2001@localhost:5434/account_service_test?sslmode=disable"
//
//	db, err := pgxpool.New(context.Background(), dbURL)
//	if err != nil {
//		t.Errorf("Failed to connect to test database: %v", err)
//		t.FailNow()
//	}
//
//	// Run migrations
//	migrationPath := "../../migrations"
//	err = postgres.MigrateUp(db, migrationPath)
//	if err != nil {
//		t.Errorf("Failed to run migrations: %v", err)
//		t.FailNow()
//	}
//
//	return db
//}
//
//func cleanupTestDB(t interface{
//	Helper()
//	Errorf(format string, args ...interface{})
//	FailNow()
//}, db *pgxpool.Pool) {
//	t.Helper()
//
//	ctx := context.Background()
//
//	_, err := db.Exec(ctx, "TRUNCATE TABLE processed_events CASCADE")
//	if err != nil {
//		t.Errorf("Failed to cleanup processed_events: %v", err)
//	}
//	_, err = db.Exec(ctx, "TRUNCATE TABLE balances CASCADE")
//	if err != nil {
//		t.Errorf("Failed to cleanup balances: %v", err)
//	}
//	_, err = db.Exec(ctx, "TRUNCATE TABLE accounts CASCADE")
//	if err != nil {
//		t.Errorf("Failed to cleanup accounts: %v", err)
//	}
//
//	db.Close()
//}
//
//func setupTestAccount(t interface{
//	Helper()
//	Errorf(format string, args ...interface{})
//	FailNow()
//}, db *pgxpool.Pool, accountID, userID string, initialBalance int64) {
//	t.Helper()
//
//	ctx := context.Background()
//
//	_, err := db.Exec(ctx, `
//        INSERT INTO accounts (account_id, user_id, currency, created_at, updated_at)
//        VALUES ($1, $2, 'USD', NOW(), NOW())
//    `, accountID, userID)
//	if err != nil {
//		t.Errorf("Failed to insert test account: %v", err)
//		t.FailNow()
//	}
//
//	_, err = db.Exec(ctx, `
//        INSERT INTO balances (account_id, amount, last_updated)
//        VALUES ($1, $2, NOW())
//    `, accountID, initialBalance)
//	if err != nil {
//		t.Errorf("Failed to insert test balance: %v", err)
//		t.FailNow()
//	}
//}
