CREATE TABLE transactions (
    id SERIAL PRIMARY KEY,
    account_no BIGINT NOT NULL,
    date DATE NOT NULL,
    transaction_details VARCHAR(255) NOT NULL,
    chip_used BOOLEAN NOT NULL,
    value_date DATE NOT NULL,
    withdrawal_amt DECIMAL(12, 2),
    deposit_amt DECIMAL(12, 2) NOT NULL,
    balance_amt DECIMAL(12, 2) NOT NULL
);

COMMENT ON TABLE transactions IS 'Table to store bank transactions data';
COMMENT ON COLUMN transactions.account_no IS 'Account number of the customer';
COMMENT ON COLUMN transactions.date IS 'Date of the transaction';
COMMENT ON COLUMN transactions.transaction_details IS 'Details of the transaction';
COMMENT ON COLUMN transactions.chip_used IS 'Indicates whether a chip was used for the transaction';
COMMENT ON COLUMN transactions.value_date IS 'Value date of the transaction';
COMMENT ON COLUMN transactions.withdrawal_amt IS 'Amount withdrawn in the transaction';
COMMENT ON COLUMN transactions.deposit_amt IS 'Amount deposited in the transaction';
COMMENT ON COLUMN transactions.balance_amt IS 'Balance amount after the transaction';
