источник правды о счетах

- читает топик 'transactions.created'
- пишет топик 'transactions.results'

```text
account.v1.AccountService/GetAccount
account.v1.AccountService/ListAccounts
account.v1.AccountService/CreateAccount
account.v1.AccountService/UpdateAccount
account.v1.AccountService/DeleteAccount
account.v1.AccountService/GetBalance
```

сущности:

```protobuf
message Account {
  string account_id = 1;
  string user_id = 2;
  Currency currency = 3;
  google.protobuf.Timestamp created_at = 4;
  google.protobuf.Timestamp updated_at = 5;
}

message Balance {
  string account_id = 1;
  double amount = 2;
  Currency currency = 3;
  google.protobuf.Timestamp last_updated = 4;
}
```