CREATE TABLE raw.exchange_api_kline (
	exchange varchar NOT NULL,
	symbol varchar NOT NULL,
	time_frame varchar NOT NULL,
	insert_ts numeric NOT NULL,
	"data" jsonb NULL
);

CREATE TABLE raw.exchange_api_instrument_info (
	exchange varchar NOT NULL,
	insert_ts numeric NOT NULL,
	"data" jsonb NULL
);