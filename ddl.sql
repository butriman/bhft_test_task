-- raw layer
CREATE TABLE raw.exchange_api_kline (
	exchange varchar NOT NULL,
	symbol varchar NULL,
	time_frame varchar NOT NULL,
	insert_ts numeric NOT NULL,
	"data" jsonb NULL
);

CREATE TABLE raw.exchange_api_instrument_info (
	exchange varchar NOT NULL,
	insert_ts numeric NOT NULL,
	"data" jsonb NULL
);

-- dm layer
CREATE TABLE spot.dim_coin (
	exchange varchar NOT NULL,
	symbol varchar NOT NULL,
	base_coin varchar NOT NULL,
	quote_coin varchar NOT NULL,
	trading_status varchar NULL,
	insert_ts numeric NULL,
	CONSTRAINT coin_pk PRIMARY KEY (exchange, symbol)
);

CREATE TABLE spot.sfct_exchange_rate (
	exchange varchar NOT NULL, -- k
	quote_coin varchar NOT NULL, -- k
	oper_dt date NOT NULL, -- k
	usd_amt numeric NULL,
	insert_ts numeric NULL,
	CONSTRAINT exchange_rate_pk PRIMARY KEY (exchange, quote_coin, oper_dt)
);

CREATE TABLE spot.tfct_coin (
	exchange varchar NOT NULL, -- k
	symbol varchar NOT NULL, -- k
	oper_dt date NOT NULL, -- k
	vol_amt numeric NULL,
	CONSTRAINT tfct_coin_pk PRIMARY KEY (exchange, symbol, oper_dt)
);