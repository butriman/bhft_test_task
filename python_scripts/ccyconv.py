from collections import defaultdict
import datetime
import pandas as pd


def build_graph_per_date(df_pairs: pd.DataFrame, target_date: datetime.datetime) -> defaultdict:
    df_date = df_pairs[df_pairs['oper_dt'] == target_date]
    graph = defaultdict(list)
    for _, row in df_date.iterrows():
        base = row['base_coin']
        quote = row['quote_coin']
        symbol = row['symbol']
        price = row['price_avg']
        graph[base].append({'coin': quote, 'symbol': symbol, 'price': price})
        # Reverse conversions (inverse rate)
        graph[quote].append({'coin': base, 'symbol': symbol, 'price': 1.0 / price})
    return graph


def find_path(graph: defaultdict, start_coin: str, goal_coin: str) -> tuple:
    """
    Finds a conversion chain from start_coin to goal_coin within the provided graph.
    It returns a tuple: (path, conversion_factor)
      - path: list of tuples (from_coin, to_coin, symbol)
      - conversion_factor: overall factor such that 1 unit of start_coin equals
                           conversion_factor units of goal_coin.
    Returns (None, None) if no path is found.
    """
    reachable = set([start_coin])
    cost = {start_coin: 0}            # Number of hops (each conversion = 1 hop)
    conv_factor = {start_coin: 1.0}     # Cumulative conversion factor
    previous = {}                     # For backtracking the path
    came_via = {}                     # Maps coin -> (symbol, price) used for conversion

    while reachable:
        # Select the coin with the lowest hop count.
        current = min(reachable, key=lambda coin: cost[coin])
        if current == goal_coin:
            path = build_path(previous, came_via, current)
            return path, conv_factor[current]
        
        reachable.remove(current)
        for edge in graph.get(current, []):
            neighbor = edge['coin']
            new_cost = cost[current] + 1
            new_conv = conv_factor[current] * edge['price']
            if neighbor not in cost or new_cost < cost[neighbor]:
                cost[neighbor] = new_cost
                conv_factor[neighbor] = new_conv
                previous[neighbor] = current
                came_via[neighbor] = (edge['symbol'], edge['price'])
                reachable.add(neighbor)
    return None, None


def build_path(previous: dict, came_via: dict, current: str) -> list:
    """
    Reconstructs the conversion chain (as a list of (from_coin, to_coin, symbol) tuples)
    from the start coin to the current (goal) coin.
    """
    path = []
    while current in previous:
        prev = previous[current]
        symbol, _ = came_via[current]
        path.append((prev, current, symbol))
        current = prev
    path.reverse()
    return path


def convert_to_usdt(graph: defaultdict, data_targets: list, goal_coin='USDT') -> pd.DataFrame:
    """
    For each coin in df_targets, finds a conversion chain from that coin to goal_coin.
    Returns a DataFrame with:
      - coin
      - conversion_path: list of (from_coin, to_coin, symbol) tuples
      - price_in_usdt: overall conversion factor (1 coin = price_in_usdt USDT)
    """
    df_targets = pd.DataFrame({'coin': data_targets})
    results = []
    for _, row in df_targets.iterrows():
        coin = row['coin']
        if coin == goal_coin:
            results.append({
                'coin': coin,
                'conversion_path': [],
                'usdt_amt': 1.0
            })
        else:
            path, conv = find_path(graph, coin, goal_coin)
            results.append({
                'coin': coin,
                'conversion_path': path,
                'usdt_amt': conv
            })
    return pd.DataFrame(results)


def rates_process(df_pairs: pd.DataFrame, df_targets: list, goal_coin: str = 'USDT') -> pd.DataFrame:
    results_by_date = []
    for oper_dt in df_pairs['oper_dt'].unique():
        graph = build_graph_per_date(df_pairs, oper_dt)
        df_conv = convert_to_usdt(graph, df_targets, goal_coin)
        df_conv['oper_dt'] = oper_dt
        results_by_date.append(df_conv)
    return pd.concat(results_by_date, ignore_index=True)