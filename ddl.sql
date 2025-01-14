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

CREATE TABLE spot.tfct_exchange_rate (
	exchange varchar NOT NULL, -- k
	coin varchar NOT NULL, -- k
	oper_dt date NOT NULL, -- k
	usdt_amt numeric NULL,
	insert_ts numeric NULL,
	CONSTRAINT exchange_rate_pk PRIMARY KEY (exchange, coin, oper_dt)
);

CREATE TABLE spot.tfct_coin (
	exchange varchar NOT NULL, -- k
	symbol varchar NOT NULL, -- k
	oper_dt date NOT NULL, -- k
	vol_amt numeric NULL,
	insert_ts numeric NULL,
	CONSTRAINT tfct_coin_pk PRIMARY KEY (exchange, symbol, oper_dt)
);


-- view

create materialized view if not exists spot.v_coin_volume as
select tc.exchange, tc.symbol, tc.oper_dt, tc.vol_amt * coalesce(ter.usdt_amt, 1.0) as vol_usdt
  from spot.tfct_coin tc 
  left join spot.dim_coin dc 
    on tc.exchange = dc.exchange 
   and tc.symbol = dc.symbol 
  left join spot.tfct_exchange_rate ter
    on dc.quote_coin = ter.coin 
   and dc.quote_coin <> 'USDT'
   and tc.oper_dt = ter.oper_dt
   and tc.exchange = ter.exchange;