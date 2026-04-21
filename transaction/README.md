сервис осуществления транзакций

- читает топик 'transactions.results'
- пишет топик 'transactions.created'

```text
transaction.v1.TransactionService/CreateTransfer
transaction.v1.TransactionService/CreateDeposit
transaction.v1.TransactionService/CreateWithdrawal
transaction.v1.TransactionService/GetTransaction
transaction.v1.TransactionService/ListTransactions
transaction.v1.TransactionService/GetTransactionStatus
```

есть три вида транзакций: снятие, пополнение и трансфер

сущность транзакции:

```protobuf
message Transaction {
  string transaction_id = 1;
  TransactionType type = 2;
  string from_account_id = 3;
  string to_account_id = 4;
  double amount = 5;
  Currency currency = 6;
  TransactionStatus status = 7;
  string idempotency_key = 8;
  string error_message = 9;
  google.protobuf.Timestamp created_at = 10;
  google.protobuf.Timestamp updated_at = 11;
  google.protobuf.Timestamp completed_at = 12;
}
```